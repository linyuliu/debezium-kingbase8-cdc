# Repository Guidelines

## Project Structure & Module Organization
- `src/main/java/` contains the connector implementation under `io.debezium.connector.kingbasees`.
- `src/main/resources/` holds runtime resources (e.g., build metadata).
- `src/test/java/` contains local demo/test entry points (for example `KingbaseTest.java`).
- `docs/` includes operational notes (for example CDC setup guidance).
- `pom.xml` defines Maven build configuration and dependencies.

## Build, Test, and Development Commands
- `mvn -q -DskipTests package` builds the connector JAR without running tests.
- `mvn -q test` runs the test suite (if present).
- Run `src/test/java/KingbaseTest.java` from your IDE for a local CDC smoke test.

## Coding Style & Naming Conventions
- Language: Java 8 source/target (see `pom.xml`).
- Indentation: 4 spaces, no tabs.
- Naming: standard Java conventions (classes `UpperCamelCase`, methods/fields `lowerCamelCase`, constants `UPPER_SNAKE_CASE`).
- Keep changes scoped to the Kingbase connector package.

## Testing Guidelines
- Tests live in `src/test/java/`.
- Prefer small, targeted tests or runnable demos that validate CDC behavior.
- Name tests with `*Test` suffix.

## Commit & Pull Request Guidelines
- No explicit commit convention is defined in this repo; use concise, imperative messages (e.g., `Fix decoderbufs parsing`).
- PRs should include a short summary, reproduction steps, and the exact Kingbase/decoder plugin versions used.

## Configuration Tips
- CDC requires logical decoding and a decoder plugin. See `docs/` for sample setup.
- For local runs, keep offset/history files in the working directory to simplify cleanup.
