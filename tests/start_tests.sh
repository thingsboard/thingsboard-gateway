pip3 install tb-rest-client
cd ../
cp docker/Dockerfile .
docker build -t tb-gateway --load .
cd tests
docker compose up -d
sleep 60
python3 prepare/add_gateway_device.py
#python3 -m unittest discover -s . -p '*_test.py'
docker compose down