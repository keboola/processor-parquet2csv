FROM python:3.12-slim
ENV PYTHONIOENCODING utf-8

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y libsnappy-dev
COPY requirements.txt /code/
RUN pip install -r /code/requirements.txt
RUN pip install flake8

COPY . /code/
WORKDIR /code/

CMD ["python", "-W", "ignore", "-u", "/code/src/component.py"]
