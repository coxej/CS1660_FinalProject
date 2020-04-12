
FROM python:3.7-slim-buster
COPY . /app
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install -r requirements.txt
ENTRYPOINT [ "python" ]
CMD [ "finalgui.py" ]