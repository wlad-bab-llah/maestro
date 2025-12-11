from airflow.models import Variable
from airflow.utils.log.secrets_masker import mask_secret

raw = Variable.get("MINIO_CONFIG")  # the JSON string
mask_secret(raw)                    # mask the whole JSON value

import json

try:
    data = json.loads(raw)
    for k, v in data.items():
        if any(word in k.lower() for word in ("bucket_name", "secret_key")):
            mask_secret(str(v))
except Exception:
    pass