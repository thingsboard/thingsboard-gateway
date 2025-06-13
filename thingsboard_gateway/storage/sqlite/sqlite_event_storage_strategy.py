#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.


from abc import ABC, abstractmethod, abstractstaticmethod
from time import sleep, monotonic
from typing import List
from thingsboard_gateway.storage.sqlite.sqlite_event_storage_pointer import Pointer


class OnInitDatabaseStrategy(ABC):
    @abstractmethod
    def initial_read_files(self):
        pass

    @abstractmethod
    def initial_write_files(self):
        pass


class PointerInitStrategy(OnInitDatabaseStrategy):

    def __init__(
            self,
            data_folder_path: str,
            default_database_name: str,
            log,
            state_file_name: str = "state.txt",
    ):
        self.pointer = Pointer(data_folder_path, log, state_file_name)
        self.default_database_name = default_database_name

    def check_should_create_or_assign_database_on_gateway_init(self) -> str:
        all_db_files = self.pointer.sort_db_files()
        if all_db_files and self.default_database_name < all_db_files[0]:
            return all_db_files[0]
        return self.default_database_name

    def initial_read_files(self) -> str:
        return self.pointer.read_database_file

    def initial_write_files(self) -> str:
        return self.pointer.write_database_file

    def update_read_database_file(self, new_filename: str) -> None:
        self.pointer.update_read_database_filename(new_filename)

    def update_write_database_file(self, new_filename: str) -> None:
        self.pointer.update_write_database_file(new_filename)

    def generate_new_file_name(self) -> str:
        return self.pointer.generate_new_file_name()


class OverSizeDbStrategyOnGatewayDisconnected(ABC):

    @abstractmethod
    def handle_oversize(self, storage: "SQLiteEventStorage") -> None:
        pass


class RotateOnOversizeDbStrategy(OverSizeDbStrategyOnGatewayDisconnected):

    def handle_oversize(self, storage: "SQLiteEventStorage") -> None:
        if storage.read_database != storage.write_database:
            timeout = 2.0
            start = monotonic()
            while (
                    not storage.write_database.process_queue.empty()
                    and monotonic() - start < timeout
            ):
                sleep(0.01)

            self.__clean_database_after_read_and_oversize(storage=storage)

            del storage.write_database
        else:
            storage.read_database.should_write = False

    @staticmethod
    def __clean_database_after_read_and_oversize(storage: "SQLiteEventStorage") -> None:
        storage.write_database.db.commit()
        storage.write_database.interrupt()
        storage.write_database.join(timeout=1)

        storage.write_database.close_db()
        storage.write_database.db.close()


class DropOnMaxCountReachedStrategy(OverSizeDbStrategyOnGatewayDisconnected):

    @staticmethod
    def handle_oversize(storage: "SQLiteEventStorage") -> bool:
        if storage.max_db_amount_reached():
            storage.is_max_db_amount_reached = True
            storage.write_database = None
            return True


class ReadOversizeStrategy(ABC):
    @abstractmethod
    def handle(self, storage: "SQLiteEventStorage") -> List[dict]:
        pass


class RotateReadOversizeStrategy(ReadOversizeStrategy):
    def handle(self, storage: "SQLiteEventStorage") -> List[dict]:
        storage.delete_oversize_db_file(
            storage.read_database.settings.data_folder_path
        )
        storage.delete_time_point = 0
        all_files = storage.rotation.pointer.sort_db_files()
        if len(all_files) > 1:
            storage.assign_existing_read_database(all_files[0])
        else:
            storage.old_db_is_read_and_write_database_in_size_limit()
        return storage.read_data()


class DropReadOversizeStrategy(ReadOversizeStrategy):
    def handle(self, storage: "SQLiteEventStorage") -> List[dict]:
        if storage.max_db_amount_reached():
            storage.is_max_db_amount_reached = True
            storage.write_database = None
            return []
