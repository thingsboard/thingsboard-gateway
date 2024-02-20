export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt --no-cache-dir
pip install pyjwt==2.6.0 --no-cache-dir
pip install tb-rest-client --no-cache-dir
cp docker/Dockerfile .
docker build -t tb-gateway --load .
docker compose --file tests/docker-compose.yml up -d
sleep 60
python3 -m unittest discover -s . -p 'test_*.py' -v
docker compose down
deactivate
rm -rf venv