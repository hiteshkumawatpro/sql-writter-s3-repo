# Offline bundle manifest

Downloaded and verified on 2026-10-09 for native Windows x64 operation.

| Component | Version | Archive | Verification |
|---|---:|---|---|
| Apache Kafka (Scala 2.13) | 4.1.2 | `archives/kafka_2.13-4.1.2.tgz` | Publisher SHA-512: `78AC6E488B1071122F9608DFDB363F6FE50E1DBBC492347002C0398DFBF77E0D8CAA5BF794C5937379721004DFE92025937D238B0FAF4EB715529417FD43B491` |
| Debezium MySQL connector | 3.5.2.Final | `archives/debezium-connector-mysql-3.5.2.Final-plugin.tar.gz` | Publisher SHA-1: `18DAA93422250FD2482FC4289011CB068E9A8D4B` |
| Eclipse Temurin JRE | 21.0.12.1+1 | `archives/temurin-jre-21-windows-x64.zip` | Local SHA-256: `D35F31E712F0FCF6AC5A093EDC90204FBFF22F720BA3950BD09D331D5E621636` |
| Python wheels | pinned by `requirements.txt` | `python-wheels/*.whl` | Installed by pip with `--no-index` |

Official download sources:

- `https://downloads.apache.org/kafka/4.1.2/`
- `https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.5.2.Final/`
- `https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jre/hotspot/normal/eclipse`

The extracted Kafka runtime, connector JARs, and Python wheels are retained so
`python manage.py setup` and all normal runtime commands require no package
download. The Kafka source archive is stored with Git LFS because it exceeds
GitHub's 100 MB per-file limit. If an offline deployment receives only its
small LFS pointer, `verify-bundle` skips the source-archive checksum and verifies
the extracted runtime instead; the archive is not required to run the pipeline.

On Windows, the bundled Java runtime is used. On Linux, `manage.py` uses the
system `java` executable when no Linux runtime exists under `vendor/java`.
