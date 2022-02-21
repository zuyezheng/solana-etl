from __future__ import annotations

import multiprocessing
import time
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import dask
from distributed import LocalCluster, Client
from neo4j import GraphDatabase, Driver

from src.load.StorageFormat import StorageFormat


class GraphBuilder:
    """
    @author zuyzheng
    """

    @staticmethod
    @contextmanager
    def with_local_cluster(
        temp_dir: str,
        n_workers: int = multiprocessing.cpu_count(),
        **kwargs
    ) -> GraphBuilder:
        config = {
            'temporary_directory': temp_dir,
            'distributed.comm.timeouts.connect': '120s',
            'distributed.comm.timeouts.tcp': '120s'
        }

        with dask.config.set(config), \
            LocalCluster(n_workers=n_workers, threads_per_worker=1) as cluster, \
            Client(cluster, timeout=120) as client:
            kwargs['client'] = client
            kwargs['n_workers'] = n_workers
            yield GraphBuilder(**kwargs)

    _driver: Driver
    _client: Client
    _n_workers: int

    def __init__(self, uri: str, user: str, password: str, client: Client, n_workers: int):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._client = client
        self._n_workers = n_workers

    def create_artifacts(
        self,
        transfers_glob: str,
        transfers_format: StorageFormat,
        destination_dir: str,
        # span in seconds used to bucket transfers for aggregation
        bucket_span: int
    ):
        """ Load transfers and compute the artifacts used for vertices and edges to store in a graph. """
        destination_path = Path(destination_dir)

        df = transfers_format.read_file(transfers_glob).repartition(npartitions=self._n_workers)

        # aggregate transfers so edges between addresses are less noisy
        aggregate_transfers = df.assign(
            # make creating a count of rows column simpler
            size=1,
            # create buckets for time
            time_bucket=(df.time // bucket_span) * bucket_span,
            earliest=df.time,
            latest=df.time
        )
        # aggregate by unique tuples per coin/token and by given time span bucket
        aggregate_transfers = aggregate_transfers.groupby(['source', 'destination', 'mint', 'time_bucket'])
        aggregate_transfers = aggregate_transfers.agg({
            'value': 'sum',
            'scale': 'first',
            'earliest': 'min',
            'latest': 'max',
            'size': 'sum'
        })
        # convert the value to a float in the right scale as an approximation due to overflows
        aggregate_transfers = aggregate_transfers.assign(
            value_approx=aggregate_transfers.value / 10**aggregate_transfers.scale
        )
        aggregate_transfers = aggregate_transfers.sort_values('size', ascending=False).reset_index()

        dask.compute(
            StorageFormat.CSV.to_file(
                df.source.append(df.destination).unique().rename('hash'),
                str(destination_path.joinpath('addresses'))
            ),
            StorageFormat.CSV.to_file(
                df.mint.unique().rename('hash'),
                str(destination_path.joinpath('mints'))
            ),
            StorageFormat.CSV.to_file(
                aggregate_transfers,
                str(destination_path.joinpath('aggregated_transfers'))
            ),
        )

    def create_graph(self, artifacts_dir: str):
        """ Create the vertices to join with edges later. """
        artifacts_path = Path(artifacts_dir)

        def create_vertices(name, file, tx):
            tx.run(
                f"""
                    LOAD CSV WITH HEADERS FROM 'file:///{file}' AS line
                    CREATE (:{name} {{
                        hash: line.hash
                    }})
                """,
                file=file
            )

        def create_index(name, tx):
            tx.run(f'CREATE INDEX {name}_hash IF NOT EXISTS FOR (n:{name}) ON (n.hash)')

        def create_edges(file, tx):
            tx.run(
                f"""
                    CALL apoc.periodic.iterate(
                        'LOAD CSV WITH HEADERS FROM "file:///{file}" AS line RETURN line', 
                        '
                        MATCH 
                            (source:Address {{hash: line.source}}), 
                            (destination:Address {{hash: line.destination}}),
                            (mint:Mint {{hash: line.mint}})
                        CREATE 
                            (source)-[:transfer {{
                                mint: line.mint,
                                value: line.value,
                                scale: toInteger(line.scale),
                                value_approx: toFloat(line.value_approx),
                                time_bucket: toInteger(line.time_bucket),
                                earliest: toInteger(line.earliest),
                                latest: toInteger(line.latest),
                                size: toInteger(line.size)
                            }}]->(destination),
                            (source)-[:mint {{
                                role: "source",
                                value: line.value,
                                scale: toInteger(line.scale),
                                value_approx: toFloat(line.value_approx),
                                time_bucket: toInteger(line.time_bucket),
                                earliest: toInteger(line.earliest),
                                latest: toInteger(line.latest),
                                size: toInteger(line.size)
                            }}]->(mint),
                            (destination)-[:mint {{
                                role: "destination",
                                value: line.value,
                                scale: toInteger(line.scale),
                                value_approx: toFloat(line.value_approx),
                                time_bucket: toInteger(line.time_bucket),
                                earliest: toInteger(line.earliest),
                                latest: toInteger(line.latest),
                                size: toInteger(line.size)
                            }}]->(mint)
                        ',
                        {{batchSize:100000, iterateList:true, parallel:false}}
                    )
                """
            )

        with self._driver.session() as session:
            # create the vertices and indices for them
            for vertex_name, file_prefix in [['Address', 'addresses'], ['Mint', 'mints']]:
                session.write_transaction(partial(
                    create_vertices, vertex_name, str(artifacts_path.joinpath(f'{file_prefix}.csv'))
                ))

                session.write_transaction(partial(
                    create_index, vertex_name
                ))

            # create the edges
            session.write_transaction(partial(
                create_edges, str(artifacts_path.joinpath('aggregated_transfers.csv'))
            ))


if __name__ == '__main__':
    with GraphBuilder.with_local_cluster('', uri='bolt://localhost:7687', user='neo4j', password='test12345') as graph_builder:
        start = time.perf_counter()
        graph_builder.create_artifacts(
            '/mnt/storage/datasets/sol/streaming/120470000_transfers.csv',
            StorageFormat.CSV,
            '/mnt/scratch_raid/solana/graph/120470000/artifacts',
            # bucket per hour
            60*60
        )
        graph_builder.create_graph('/graph/120470000/artifacts')
        print((time.perf_counter() - start))
