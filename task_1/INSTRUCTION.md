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

**Учетные данные:**
- Пользователь для входа извне: `ubuntu` (аутентификация по ключу `big_data_rsa`) + пароль


---

### 1. Подключение к узлу для входа

```bash
# Подключаемся к внешнему узлу используя предоставленный ключ
ssh -i .ssh/big_data_rsa ubuntu@178.236.25.105
```
### 2. Настройка SSH доступа
Настроим беспарольный SSH доступ на все узлы. Настроим его с узла jn:

```bash
# Генерируем SSH ключи
ssh-keygen

# Добавляем публичный ключ в authorized_keys на текущем узле
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys

# Копируем ключи на все узлы кластера
scp ~/.ssh/authorized_keys 192.168.10.31:/home/ubuntu/.ssh/
scp ~/.ssh/authorized_keys 192.168.10.29:/home/ubuntu/.ssh/
scp ~/.ssh/authorized_keys 192.168.10.30:/home/ubuntu/.ssh/
```

### 3. Настройка /etc/hosts
Для удобства работы с именами узлов, добавляем записи в /etc/hosts на каждой машине:


```bash
sudo vim /etc/hosts
```
Файл должен выглядеть так:
```bash
127.0.1.1 team-08-jn

192.168.10.51 jn
192.168.10.31 nn
192.168.10.29 dn-00
192.168.10.30 dn-01
```

Аналогично для всех остальных узлов, например для nn

```bash
ssh nn
sudo vim /etc/hosts
exit
```
Файл для nn будет выглядеть так:
```bash
127.0.1.1 team-08-nn

192.168.10.51 jn
192.168.10.31 nn
192.168.10.29 dn-00
192.168.10.30 dn-01
```
### 4. Создание пользователя hadoop на всех узлах
Hadoop сервисы должны запускаться от отдельного пользователя. Создадим пользователя hadoop на каждом узле:

```bash
# На узле jn
sudo adduser hadoop
```
Аналогично для всех остальных узлов, например для nn
```bash
ssh nn
sudo adduser hadoop
exit
```

Затем настроим беспарольный доступ между всеми узлами

```bash
# Переключаемся на пользователя hadoop на узле jn
sudo -i -u hadoop

# Генерируем ключ и добавляем его в authorized_keys
ssh-keygen
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys

# Разносим по всем узлам
scp -r .ssh/ nn:/home/hadoop
scp -r .ssh/ dn-00:/home/hadoop
scp -r .ssh/ dn-01:/home/hadoop
```

### 5. Установка и настройка Hadoop
1. Скачивание Hadoop на узел jn 
`Все еще под пользователем hadoop`
```bash
# Скачиваем Hadoop версии 3.4.1
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
# Разносим на все остальных узлы
scp hadoop-3.4.1.tar.gz nn:/home/hadoop/
scp hadoop-3.4.1.tar.gz dn-00:/home/hadoop/
scp hadoop-3.4.1.tar.gz dn-01:/home/hadoop/

# Распаковываем 
tar -xzf hadoop-3.4.1.tar.gz 
# На всех остальных узлах
for host in nn dn-00 dn-01; do 
	ssh hadoop@$host tar -xzf hadoop-3.4.1.tar.gz;
done
```
2. Настройка Hadoop (от пользователя hadoop)

```bash
# Смотрим где лежит java
which java
# output: /usr/bin/java
readlink -f /usr/bin/java
# output: /usr/lib/jvm/java-8-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> .profile
echo 'export HADOOP_HOME=/home/hadoop/hadoop-3.4.1' >> .profile
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> .profile

# Применим изменения
source .profile

# Проверим
hadoop version

# Копируем на все узлы
scp .profile nn:/home/hadoop
scp .profile dn-00:/home/hadoop/
scp .profile dn-01:/home/hadoop/
```
### 6. Настройка конфигурационных файлов
Зайдем в `hadoop-3.4.1/etc/hadoop` и настроим конфигурационные файлы
```bash
cd hadoop-3.4.1/etc/hadoop
```
```bash
vim hadoop-env.sh
# Нужно добавить строчку с JAVA_HOME
```
В итоге в этом файле должно быть:
```bash
....
# Many of the options here are built from the perspective that users
# may want to provide OVERWRITING values on the command line.
# For example:
#
#  JAVA_HOME=/usr/java/testing hdfs dfs -ls
#
# Therefore, the vast majority (BUT NOT ALL!) of these defaults
# are configured for substitution and not append.  If append
# is preferable, modify this file accordingly.

JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
....
```
#### core-site.xml - определяет NameNode
```bash
 vim core-site.xml
```
В итоге в этом файле должно быть:
```bash
<?xml version="1.0" encoding="UTF-8"?>
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
        <name>fs.defaultFS</name>
        <value>hdfs://nn:9000</value>
    </property>
</configuration>
```
#### hdfs-site.xml - настройки HDFS
```bash
vim hdfs-site.xml
```
В итоге в этом файле должно быть:
```bash
<?xml version="1.0" encoding="UTF-8"?>
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
        <name>dfs.replication</name>
        <value>3</value>
    </property>
</configuration>
```
#### workers - список DataNode узлов
```bash
vim workers
```
В итоге в этом файле должно быть:
```bash
nn
dn-00
dn-01
```

Осталось скопировать эти файлы на все остальные узлы, например для nn: 
```bash
scp hadoop-env.sh nn:/home/hadoop/hadoop-3.4.1/etc/hadoop/
scp core-site.xml nn:/home/hadoop/hadoop-3.4.1/etc/hadoop/
scp hdfs-site.xml nn:/home/hadoop/hadoop-3.4.1/etc/hadoop/
scp workers nn:/home/hadoop/hadoop-3.4.1/etc/hadoop/
```
### 7. Запуск кластера
Переходим на NameNode (nn) и форматируем
```bash
ssh nn
cd hadoop-3.4.1/
bin/hdfs namenode -format
```
Запускаем HDFS кластер
```bash
sbin/start-dfs.sh
```
### 8. Проверка работоспособности

1. Проверка через jps
На nn
```bash
jps
# output должен содержать:
# NameNode
# SecondaryNameNode
# DataNode
```
На dn-00, dn-01
```bash
jps
# output должен содержать:
# DataNode
```
2. Проверка через веб-интерфейс
На локальной машине:
```bash
ssh -i .ssh/big_data_rsa -L 9870:nn:9870 ubuntu@178.236.25.105
```
После установки туннеля откройте браузер и перейдите по адресу: http://localhost:9870