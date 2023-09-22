use super::delimited::from_delimited_data;

use super::util::{get_env_var_from_engine, get_runtime};
use nu_engine::CallExt;
use nu_protocol::ast::Call;
use nu_protocol::engine::{Command, EngineState, Stack};
use nu_protocol::{
    Category, Example, PipelineData, ShellError, Signature, Span, Spanned, SyntaxShape, Value,
};

use csv::Trim;
use predicates::prelude::*;

#[derive(Clone)]
pub struct Ioxsql;

const SQL_PARSER_ERROR: &str = "Error running remote query:";

impl Command for Ioxsql {
    fn name(&self) -> &str {
        "ioxsql"
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::build("ioxsql")
            .required(
                "query",
                SyntaxShape::String,
                "SQL to execute against the database",
            )
            .named(
                "dbname",
                SyntaxShape::String,
                "name of the database to search over",
                Some('d'),
            )
            .category(Category::Filters)
    }

    fn usage(&self) -> &str {
        "Sql query against the Iox Database."
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let sql: Spanned<String> = call.req(engine_state, stack, 0)?;
        let db: Option<String> = call.get_flag(engine_state, stack, "dbname")?;

        let dbname = if let Some(name) = db {
            name
        } else {
            get_env_var_from_engine(stack, engine_state, "IOX_DBNAME").unwrap()
        };

        let check_sql_result = tokio_block_sql(&dbname, &sql).unwrap_or("dog".to_string());

        let sql = check_sql_result.clone();
        let value_predicate = predicate::str::contains(SQL_PARSER_ERROR);
        let parse_error = value_predicate.eval(&check_sql_result);

        if parse_error {
            println!("check_sql_result = {:?}\n", check_sql_result);
            return Err(ShellError::GenericError(
                "Your SQL is not properly formed ".to_string(),
                "Please enter an SQL string that will execute a query".to_string(),
                Some(call.head),
                None,
                Vec::new(),
            ));
        }

        let no_infer = false;
        let noheaders = false;
        let separator: char = ',';
        let trim = Trim::None;

        let input = PipelineData::Value(Value::string(sql.to_string(), call.head), None);

        let name = Span::new(0, 0);
        let config = engine_state.get_config();

        from_delimited_data(noheaders, no_infer, separator, trim, input, name, config)
    }

    fn examples(&self) -> Vec<Example> {
        vec![
            Example {
                description: "Run an sql query against the bananas database",
                example: r#"ioxsql -d bananas "select * from cpu"#,
                result: None,
            },
            Example {
                description: "Run an sql query against the default database",
                example: r#"ioxsql "select * from cpu"#,
                result: None,
            },
        ]
    }
}

pub fn tokio_block_sql(dbname: &String, sql: &Spanned<String>) -> Result<String, std::io::Error> {
    use crate::iox::Nuclient;
    use influxdb_iox_client::connection::Builder;
    let num_threads: Option<usize> = None;
    let tokio_runtime = get_runtime(num_threads)?;

    let sql_result = tokio_runtime.block_on(async move {
        let connection = Builder::default()
            .build("http://127.0.0.1:8082")
            .await
            .expect("client should be valid");

        let mut repl = Nuclient::new(connection);
        repl.use_database(dbname.to_string());
        let _output_format = repl.set_output_format("csv");

        let rsql = repl.run_sql(sql.item.to_string()).await;

        match rsql {
            Ok(res) => res,
            Err(error) => error.to_string(),
        }
    });

    Ok(sql_result)
}
