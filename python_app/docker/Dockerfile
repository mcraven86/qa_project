from python:3.12.3

#setting the working directory within the container

WORKDIR /app

#copy everything from working directory to app folder
COPY . /app

#On starting what should docker do?
ENTRYPOINT["python", "docker_demo.py"]