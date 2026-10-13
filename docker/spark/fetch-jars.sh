#!/bin/sh
# Baixa o runtime do Iceberg e o conector S3A para $1 e confere os checksums SHA-256 fixados, para que um
# download adulterado ou corrompido faça o build da imagem falhar.
# As versões precisam bater com o Hadoop embutido no PySpark: Spark 4.1 -> Hadoop 3.4.2 -> AWS SDK v2 2.29.52.
set -eu

dest=${1:?uso: fetch-jars.sh <pasta de destino>}
repo=${MAVEN_REPO:-https://repo1.maven.org/maven2}

mkdir -p "$dest"
cd "$dest"

curl -fsSLO "$repo/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar"
curl -fsSLO "$repo/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar"
curl -fsSLO "$repo/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
curl -fsSLO "$repo/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.2.1/analyticsaccelerator-s3-1.2.1.jar"

sha256sum -c - <<EOF
3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82  iceberg-spark-runtime-4.1_2.13-1.12.0.jar
d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9  hadoop-aws-3.4.2.jar
62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c  bundle-2.29.52.jar
11029552e37408e41e689b20150f4b372e0b4f232de321ccafd0fcf443cdfb5a  analyticsaccelerator-s3-1.2.1.jar
EOF

chmod 644 ./*.jar
