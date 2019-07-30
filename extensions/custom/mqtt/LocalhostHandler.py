from . import ExtensionInterface


class Extension(ExtensionInterface.ExtensionInterface):
    def create_rpc_request_to_device(self, request_body):
        mapping = {
            "device_name_filter": "*",
            "request_topic_expression": "{device_name}/response/{method}/{request_id}",
            "is_two_way": False,
            # todo add example with parameters
            # "params": "{params}"
        }
        return mapping
