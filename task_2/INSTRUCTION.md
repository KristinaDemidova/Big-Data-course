# Развертывание HDFS кластера
## Практическое задание №1

### Информация о кластере

**Внешний доступ:**
- Узел для входа (jn): `178.236.25.105`

**Внутренние адреса узлов:**
- jn: `192.168.10.51`
- nn: `192.168.10.31`
- dn-00: `192.168.10.29`
- dn-01: `192.168.10.30`

---

### 1. Cоздание конфигурационных файлов

На **NameNode** отредактируйте следующие файлы в каталоге `$HADOOP_HOME/etc/hadoop/`:

- **`mapred-site.xml`** — указывает использовать YARN в качестве фреймворка выполнения:
```xml
    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <!--
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License. See accompanying LICENSE file.
    -->

    <!-- Put site-specific property overrides in this file. -->

    <configuration>
        <property>
            <name>mapreduce.framework.name</name>
            <value>yarn</value>
        </property>
        <property>
            <name>mapreduce.application.classpath</name>
            <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
        </property>
    </configuration>
```

- **`yarn-site.xml`** — основные настройки YARN:
```xml
    <?xml version="1.0"?>
    <!--
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License. See accompanying LICENSE file.
    -->
    <configuration>
        <property>
            <name>yarn.nodemanager.aux-services</name>
            <value>mapreduce_shuffle</value>
        </property>
        <property>
            <name>yarn.nodemanager.env-whitelist</name>
            <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
        </property>
        <property>
            <name>yarn.resourcemanager.hostname</name>
            <value>nn</value>
        </property>
        <property>
            <name>yarn.resourcemanager.address</name>
            <value>nn:8032</value>
        </property>
        <property>
            <name>yarn.resourcemanager.resource-tracker.address</name>
            <value>nn:8031</value>
        </property>
    </configuration>
```

### 2. Копирование конфигураций на другие узлы 
Пример для mapred-site и dn-00 ноды
```bash
    scp mapred-site.xml   dn-00:/home/hadoop/hadoop-3.4.1/etc/hadoop/
```
По аналогии копируем yarn-site.xml на все нужные ноды

### 3. Запуск YARN и HistoryServer
На NameNode нужно выполнить:
```bash
    hadoop-3.4.1/sbin/start-yarn.sh      # запуск ResourceManager и NodeManager на всех узлах
    hadoop-3.4.1/bin/mapred --daemon start historyserver # запуск HistoryServer
```

### 4. Проверка запущенных сервисов
Запустите команду
```bash
jps
```

Для NameNode вывод должен выглядеть

```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
JobHistoryServer
```

Для DataNode вывод должен выглядеть

```
DataNode
NodeManager
```

### 5. Доступ к веб-интерфейсам

Для внешнего доступа к веб-интерфейсам кластера используем SSH-туннель через jump node. Порты по умолчанию:

- **NameNode Web UI** — порт `9870`
- **ResourceManager Web UI** — порт `8088`
- **JobHistoryServer Web UI** — порт `19888`

```bash
ssh -L 9870:192.168.10.31:9870 -L 8088:192.168.10.31:8088 -L 19888:192.168.10.31:19888 ubuntu@178.236.25.105
```

После выполнения команды вы сможете открыть в браузере на локальной машине для проверки статуса сервисов:

- http://localhost:9870 — интерфейс NameNode
- http://localhost:8088 — интерфейс ResourceManager
- http://localhost:19888 — интерфейс JobHistoryServer
