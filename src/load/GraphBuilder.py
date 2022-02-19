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

    def entities_from_transfers(self, transfers_glob: str, transfers_format: StorageFormat, destination_dir: str):
        """ Load transfers and extract out unique entities to create. """
        destination_path = Path(destination_dir)

        df = transfers_format.read_file(transfers_glob).repartition(npartitions=self._n_workers)
        dask.compute(
            StorageFormat.CSV.to_file(
                df.source.append(df.destination).unique().rename('hash'),
                str(destination_path.joinpath('addresses'))
            ),
            StorageFormat.CSV.to_file(
                df.mint.unique().rename('hash'),
                str(destination_path.joinpath('mints'))
            )
        )

    def create_vertices(self, vertices_dir: str):
        """ Create the vertices to join with edges later. """
        vertices_path = Path(vertices_dir)

        def create(name, file, tx):
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

        with self._driver.session() as session:
            for vertex_name, file_prefix in [['Address', 'addresses'], ['Mint', 'mints']]:
                session.write_transaction(partial(
                    create, vertex_name, str(vertices_path.joinpath(f'{file_prefix}.csv'))
                ))

                session.write_transaction(partial(
                    create_index, vertex_name
                ))

    def create_transfer_edges(self, transfers_csv_path: str):
        def create(file, tx):
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
                                value: toInteger(line.value),
                                scale: toInteger(line.scale),
                                time: toInteger(line.time),
                                transaction: line.transaction,
                                blockhash: line.blockhash
                            }}]->(destination),
                            (source)-[:mint {{
                                role: "source",
                                value: toInteger(line.value),
                                scale: toInteger(line.scale),
                                time: toInteger(line.time),
                                transaction: line.transaction,
                                blockhash: line.blockhash
                            }}]->(mint),
                            (destination)-[:mint {{
                                role: "destination",
                                value: toInteger(line.value),
                                scale: toInteger(line.scale),
                                time: toInteger(line.time),
                                transaction: line.transaction,
                                blockhash: line.blockhash
                            }}]->(mint)
                        ',
                        {{batchSize:100000, iterateList:true, parallel:false}}
                    )
                """
            )

        with self._driver.session() as session:
            session.write_transaction(partial(create, transfers_csv_path))


if __name__ == '__main__':
    with GraphBuilder.with_local_cluster('', uri='bolt://localhost:7687', user='neo4j', password='test12345') as graph_builder:
        start = time.perf_counter()
        graph_builder.entities_from_transfers(
            '/mnt/scratch_raid/solana/streaming/120470000_transfers.csv',
            StorageFormat.CSV,
            '/mnt/scratch_raid/solana/graph/120470000/vertices'
        )
        graph_builder.create_vertices('/graph/120470000/vertices')
        graph_builder.create_transfer_edges('/streaming/120470000_transfers.csv')
        print((time.perf_counter() - start))
