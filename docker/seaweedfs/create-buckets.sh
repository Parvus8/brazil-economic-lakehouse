#!/bin/sh
# Cria os buckets do lake pelo shell de administração do SeaweedFS. Pode rodar a cada inicialização.
# Logo após a inicialização, o weed shell às vezes encerra antes de alcançar o filer, então cada bucket é
# tentado de novo.
set -eu

for bucket in ${BUCKETS:-bronze silver gold}; do
  attempt=1
  # O código de saída não é usado: o weed shell também falha quando o bucket já existe, o que aqui é normal.
  # As mensagens procuradas abaixo são as do próprio SeaweedFS, em inglês.
  until output=$(printf 's3.bucket.create -name %s\n' "$bucket" | weed shell -master=seaweedfs:9333 2>&1 || true) \
      && echo "$output" | grep -qE "created bucket|already exists"; do
    if [ "$attempt" -ge 10 ]; then
      echo "não foi possível criar o bucket $bucket: $output"
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep 3
  done
  echo "bucket $bucket pronto"
done
