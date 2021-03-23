FROM python:3.7.2-slim
ENV PYTHONIOENCODING utf-8

COPY . /code/
RUN pip install flake8

RUN pip install -r /code/requirements.txt

WORKDIR /code/


CMD ["python", "-u", "/code/src/component.py"]
