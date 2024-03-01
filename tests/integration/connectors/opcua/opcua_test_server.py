# import asyncio
# import logging
#
# from asyncua import Server
#
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')
# log = logging.getLogger("TEST")
#
#
# class OpcUaTestServer:
#     def __init__(self):
#         self._server = None
#         self._idx = None
#         self._ready = False
#         self._stop = False
#         self._iteration = 0
#
#     @property
#     def is_ready(self):
#         return self._ready
#
#     @property
#     def is_stopped(self):
#         return self._stop and not self._ready
#
#     @property
#     def server(self):
#         return self._server
#
#     @property
#     def idx(self):
#         return self._idx
#
#     def stop(self):
#         self._server.stop()
#         self._stop = True
#
#     async def create_nodes(self):
#         pass
#
#     async def update_nodes(self, iteration):
#         pass
#
#     @property
#     def iteration(self):
#         return self._iteration
#
#     async def run(self, wait_for_clients=True):
#         log.info('Setup test server')
#         self._server = Server()
#         await self._server.init()
#         self._server.set_endpoint("opc.tcp://0.0.0.0:4841/freeopcua/server/")
#
#         # setup our own namespace, not really necessary but should as spec
#         uri = 'http://examples.freeopcua.github.io'
#         self._idx = await self._server.register_namespace(uri)
#
#         await self._server.load_data_type_definitions()
#
#         await self.create_nodes()
#
#         log.info('Starting test server')
#         async with self._server:
#             self._ready = True
#
#             if wait_for_clients:
#                 while len(self._server.bserver.clients) == 0 and not self._stop:
#                     await asyncio.sleep(1)
#                 if len(self._server.bserver.clients) > 0:
#                     log.info('Client connected')
#
#             while not self._stop:
#                 await self.update_nodes(self._iteration)
#                 self._iteration += 1
#
#         self._ready = False
#         log.info('Stopped test server')
