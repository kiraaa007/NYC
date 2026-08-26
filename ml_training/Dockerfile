FROM python:3.11-slim

WORKDIR /trainer

COPY requirements.txt /trainer/requirements.txt
RUN pip install --no-cache-dir -r /trainer/requirements.txt

COPY train_deployable_model.py /trainer/train_deployable_model.py

CMD ["python", "/trainer/train_deployable_model.py"]
