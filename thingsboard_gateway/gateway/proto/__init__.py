#      Copyright 2022. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

# Use the following command to generate python code from proto file;

#$ python3 -m grpc_tools.protoc -Ithingsboard_gateway/gateway/proto --python_out=thingsboard_gateway/gateway/proto/ --grpc_python_out=thingsboard_gateway/gateway/proto/ thingsboard_gateway/gateway/proto/messages.proto

# Update file messages_pb2_grpc.py:

# Replace:
# import messages_pb2 as messages__pb2
# With:
# import thingsboard_gateway.gateway.proto.messages_pb2 as messages__pb2
