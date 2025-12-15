FROM astrocrpublic.azurecr.io/runtime:3.1-8

# Adicione estas linhas para liberar o Java 17 para o Spark
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                      --add-opens=java.base/java.io=ALL-UNNAMED \
                      --add-opens=java.base/java.net=ALL-UNNAMED \
                      --add-opens=java.base/java.nio=ALL-UNNAMED \
                      --add-opens=java.base/java.util=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                      --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                      --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"