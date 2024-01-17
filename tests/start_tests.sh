export PYTHONPATH="${PYTHONPATH}:$(pwd):$(pwd)/../"
python3 -m venv venv
source venv/bin/activate
pip install -r ../requirements.txt --no-cache-dir
pip install pyjwt --no-cache-dir
pip install tb-rest-client --no-cache-dir
cd ../
cp docker/Dockerfile .
#docker build -t tb-gateway --load .
cd tests
docker compose up -d
sleep 60
python3 -m unittest discover -s . -p 'test_*.py' -v
docker compose down
deactivate
rm -rf venv