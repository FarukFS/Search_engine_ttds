#pip install --no-cache-dir --extra-index-url http://$PYPI_USERNAME:$PYPI_PASSWORD@$PYPI_HOST:$PYPI_PORT/ --trusted-host $PYPI_HOST $PACKAGE_NAME

#python3 ./init/create_index.py

gunicorn --bind 0.0.0.0:5001 --timeout 300 main_app:flask_app 