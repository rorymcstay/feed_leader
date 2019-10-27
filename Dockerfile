FROM python:3.6-alpine

RUN mkdir -p /home

WORKDIR /home

ADD requirements.txt ./requirements.txt

RUN python -m pip install pip
RUN pip install -r ./requirements.txt


# Installing packages
# Copying over necessary files
COPY src ./src
COPY settings.py ./settings.py
COPY leader.py ./app.py

# Entrypoint
CMD ["python", "./app.py" ]
