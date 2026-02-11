# Debezium Connector for Kingbase

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Debezium Version](https://img.shields.io/badge/debezium-1.5.4--Final-brightgreen.svg)](https://debezium.io/)
[![Java 8+](https://img.shields.io/badge/java-8+-blue.svg)](https://adoptopenjdk.net/)

金仓数据库（Kingbase）的 Debezium CDC 连接器，基于日志的变更数据捕获技术实现数据实时同步。

## 📋 目录

- [特性](#特性)
- [架构](#架构)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [本地测试](#本地测试)
- [Flink CDC 集成](#flink-cdc-集成)
- [开发指南](#开发指南)
- [贡献](#贡献)

## ✨ 特性

- **实时数据同步**：基于 WAL 日志实现实时变更捕获
- **丰富数据类型支持**：支持 Kingbase 的各种数据类型
- **高可用性**：支持复制槽管理和故障恢复
- **灵活部署**：可嵌入应用或独立运行
- **多种快照模式**：支持不同场景下的初始化策略

## 🏗️ 架构

本连接器基于 [Debezium 1.5.4.Final](https://debezium.io/) 构建，采用以下核心技术：

- **Logical Decoding**：利用 Kingbase 的逻辑解码功能
- **Protobuf 协议**：使用 decoderbufs 插件进行高效序列化
- **复制槽机制**：确保数据变更的可靠传输
- **Schema 管理**：自动处理表结构变更

## 🚀 快速开始

### 1. 环境准备

确保 Kingbase 数据库已启用逻辑复制：

```sql
-- 修改 kingbase.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- 重启数据库服务
```

### 2. 创建复制槽

```sql
SELECT * FROM sys_create_logical_replication_slot('dbz_kingbase_slot', 'decoderbufs');
```

### 3. Maven 依赖

```xml
<dependency>
    <groupId>io.debezium</groupId>
    <artifactId>debezium-connector-kingbase</artifactId>
    <version>1.5.4.Final</version>
</dependency>
```

### 4. 基本使用

```java
Properties props = new Properties();
props.setProperty("name", "kingbase-connector");
props.setProperty("connector.class", "io.debezium.connector.kingbasees.PostgresConnector");
props.setProperty("database.hostname", "localhost");
props.setProperty("database.port", "54321");
props.setProperty("database.user", "kingbase");
props.setProperty("database.password", "password");
props.setProperty("database.dbname", "test");
props.setProperty("table.include.list", "public.users");

DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
    .using(props)
    .notifying(record -> {
        System.out.println("Received record: " + record);
    })
    .build();

ExecutorService executor = Executors.newSingleThreadExecutor();
executor.submit(engine);
```

## ⚙️ 配置说明

### 核心配置项

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `database.hostname` | 数据库主机地址 | localhost |
| `database.port` | 数据库端口 | 54321 |
| `database.user` | 数据库用户名 | - |
| `database.password` | 数据库密码 | - |
| `database.dbname` | 数据库名 | - |
| `plugin.name` | 逻辑解码插件 | decoderbufs |
| `slot.name` | 复制槽名称 | debezium |
| `table.include.list` | 包含的表列表 | - |
| `snapshot.mode` | 快照模式 | initial |

### 快照模式

- `initial`：首次运行时进行快照
- `never`：从不进行快照，只捕获增量变更
- `always`：总是进行快照
- `initial_only`：只进行初始快照
- `exported`：使用导出快照
- `custom`：自定义快照策略

## 🧪 本地测试

项目提供了完整的测试套件和演示程序：

### 运行测试

```bash
# 编译项目
mvn clean package -DskipTests

# 运行单元测试
mvn test
```

### 演示程序

参考 [`KingbaseTest.java`](src/test/java/KingbaseTest.java) 进行本地测试：

```bash
# 设置环境变量
export KB_HOST=localhost
export KB_PORT=54321
export KB_USER=kingbase
export KB_PASSWORD=password
export KB_DB=test
export KB_TABLES=public.users

# 运行测试
mvn exec:java -Dexec.mainClass="KingbaseTest"
```

## 🔌 Flink CDC 集成

本连接器可与 Apache Flink CDC 完美集成：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Properties debeziumProps = new Properties();
debeziumProps.setProperty("connector.class", "io.debezium.connector.kingbasees.PostgresConnector");
debeziumProps.setProperty("database.hostname", "localhost");
debeziumProps.setProperty("database.port", "54321");
debeziumProps.setProperty("database.user", "kingbase");
debeziumProps.setProperty("database.password", "password");
debeziumProps.setProperty("database.dbname", "test");
debeziumProps.setProperty("table.include.list", "public.users");

DataStreamSource<String> stream = env
    .addSource(new FlinkCdcSourceFunction(debeziumProps))
    .setParallelism(1);

stream.print();
env.execute("Kingbase CDC Job");
```

## 👨‍💻 开发指南

### 项目结构

```
src/
├── main/
│   ├── java/io/debezium/connector/kingbasees/
│   │   ├── connection/          # 数据库连接相关
│   │   ├── data/               # 数据类型处理
│   │   ├── snapshot/           # 快照功能
│   │   └── spi/                # 服务提供接口
│   ├── proto/                  # Protobuf 定义
│   └── resources/              # 资源文件
└── test/                       # 测试代码
    └── java/
        └── KingbaseTest.java   # 主要测试入口
```

### 构建项目

```bash
# 清理并编译
mvn clean compile

# 打包（跳过测试）
mvn package -DskipTests

# 安装到本地仓库
mvn install -DskipTests
```

### 代码规范

- 使用 Java 8 语法
- 遵循 Debezium 代码风格
- 添加必要的 Javadoc 注释
- 编写单元测试

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发流程

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 Apache License 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📚 相关文档

- [Kingbase CDC 配置指南](docs/kingbase_cdc_setup.md)
- [Kingbase -> Doris（Spring Boot 主工程方案）](docs/kingbase_to_doris_sync.md)
- [SpringBoot 2.7 独立示例（可选）](examples/springboot27-kb-doris-demo/README.md)
- [Rich Types 测试步骤](docs/kingbase_cdc_rich_types_steps.md)
- [SQL 运维脚本](docs/sql/)
