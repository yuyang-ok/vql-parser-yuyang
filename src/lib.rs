use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

pub fn parse(sql: &str) -> Result<Vec<VQL>, ParserError> {
    let dialect = GenericDialect {};
    let parser = Parser::new(&dialect);
    let mut parser = Parser::try_with_sql(parser, sql)?;
    let mut ret = Vec::new();
    while parser.peek_token().token != Token::EOF {
        match parser.peek_token().token {
            Token::Word(x)
                if x.value.clone().to_lowercase() == "create" || {
                    let t = parser.peek_nth_token(1).token;
                    match t {
                        Token::Word(w) if w.value.to_lowercase() == "datasource" => true,
                        _ => false,
                    }
                } =>
            {
                // here is create datasource statement.
                let v = parse_create_datasource(&mut parser)?;
                ret.push(VQL::V(v));
            }
            _ => {
                // fallback to parse statement
                let statement = parser.parse_statement()?;
                ret.push(VQL::Sql(statement))
            }
        }
        if parser.peek_token() == Token::SemiColon {
            parser.next_token();
        }
    }
    Ok(ret)
}

fn parse_create_datasource(parser: &mut Parser) -> Result<V, ParserError> {
    // consume create
    parser.next_token();
    // consume datasource.
    parser.next_token();
    let table_name: String;
    {
        match parser.peek_token().token {
            Token::Word(w) => {
                table_name = w.value.clone();
            }
            _ => return Err(ParserError::ParserError("expect a table_name".to_string())),
        }
        parser.next_token();
    }

    // config keyword
    {
        match parser.peek_token().token {
            Token::Word(w) if w.value.to_lowercase() == "config" => {}
            _ => return Err(ParserError::ParserError("expect a config".to_string())),
        }
        parser.next_token();
    }
    // (
    let mut config_parameters = Vec::new();

    {
        match parser.peek_token().token {
            Token::LParen => {}
            _ => unreachable!(),
        }
        parser.next_token();
        while parser.peek_token().token != Token::RParen {
            config_parameters.push(parse_config_parameters(parser)?);
        }
        // consume )
        parser.next_token();
    }

    Ok(V::CrateDataSource {
        table_name: table_name,
        config_parameters: config_parameters,
    })
}
#[derive(Debug)]
pub struct ConfigParameter {
    name: String,
    value: String,
}

// identifer = 'xxx'
fn parse_config_parameters(parser: &mut Parser) -> Result<ConfigParameter, ParserError> {
    let identifer;
    match parser.peek_token().token {
        Token::Word(w) => {
            identifer = w.value.clone();
        }
        _ => return Err(ParserError::ParserError("expect a identifer".to_owned())),
    }
    parser.next_token();
    parser.expect_token(&Token::Eq)?;
    let value: String;
    match parser.peek_token().token {
        Token::SingleQuotedString(w) => {
            value = w.clone();
        }
        _ => {
            return Err(ParserError::ParserError("expect a value".to_string()));
        }
    }
    parser.next_token();
    Ok(ConfigParameter {
        name: identifer,
        value,
    })
}

#[derive(Debug)]
pub enum VQL {
    V(V),
    Sql(Statement),
}

#[derive(Debug)]
pub enum V {
    CrateDataSource {
        table_name: String,
        config_parameters: Vec<ConfigParameter>,
    },
}

#[cfg(test)]
#[test]
fn test_parse_data_source() {
    let x = parse(
        r#"
    CREATE DATASOURCE user
    CONFIG(
        DriverClassName = 'com.mysql.jdbc.Driver'
        DataBaseUri = 'jdbc:mysql://localhost:3306/acme_crm'
        UserName = 'acme_user'
        UserPassword = 'xxxx'
        DatabaseName = 'mysql'
        DatabaseVersion = '8'

    )
    

    select * from user;
    "#,
    )
    .unwrap();
    println!("{:?}", x);
}
