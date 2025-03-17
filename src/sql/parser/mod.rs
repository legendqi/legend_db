use std::iter::Peekable;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

mod lexer;
mod ast;

pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable()
        }
    }

    // 解析，获取到抽象语法树
    pub fn parse(&mut self) -> LegendDBResult<Statement> {
        let stmt = self.parse_statement()?;
        Ok(stmt)
    }
    
    fn parse_statement(&mut self) -> LegendDBResult<Statement> {
        match self.custom_peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(token) => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token))),
            None => Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())),
            _ => Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())),
            // Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            // Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            // Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            // Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            // Some(Token::Keyword(Keyword::Alter)) => self.parse_alter(),
            // Some(Token::Keyword(Keyword::Show)) => self.parse_show(),
        }
    }

    // 解析DDL类型
    fn parse_ddl(&mut self) -> LegendDBResult<Statement> {
        match self.custom_next()? {
            // create 之后为table
            Token::Keyword(Keyword::Create) => match self.custom_next()? {
                Token::Keyword(Keyword::Table) => {
                    self.parse_create_table()
                },
                Token::Keyword(Keyword::Database) => {
                    self.parse_create_database();
                },
                token => Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()))
            },
            Token::Keyword(Keyword::Drop) => match self.custom_next()? {
                Token::Keyword(Keyword::Table) => self.parse_drop_table(),
                Token::Keyword(Keyword::Database) => self.parse_drop_database(),
                token => Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()))
            },
            token => Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()))
        }

    }

    /// 解析create table
    fn parse_create_table(&mut self) -> LegendDBResult<Statement> {
        // 期望是一个table的名字

    }

    fn parse_create_database(&mut self) -> LegendDBResult<Statement> {
        match self.next_ident()? {
            Some(Token::Identifier(ident)) => {
                // 创建数据库
                match ident.as_str() {
                    "database" => {
                        Ok(Statement::CreateDatabase {
                            name: ident,
                        })
                    },
                    "table" => {
                        Ok(Statement::CreateTable {
                            name: ident,
                        })
                    }
                }
            },
            None => Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())),
            _ => Err(LegendDBError::Parser("[Parser] Unexpected token".to_string()))
        }
    }

    fn custom_peek(&mut self) -> LegendDBResult<Option<Token>> {
        //transpose 方法作用是将 Option<Result<T, E>> 转换为 Result<Option<T>, E>
        // Option<Result<T, E>> 转换为 Result<Option<T>, E>
        // 如果 Option 是 Some(Ok(value))，则返回 Ok(Some(value))。
        // 如果 Option 是 Some(Err(error))，则返回 Err(error)。
        // 如果 Option 是 None，则返回 Ok(None)。
        // Result<Option<T>, E>> 转换为 Option<Result<T, E>>
        // 如果 Result 是 Ok(Some(value))，则返回 Some(Ok(value))。
        // 如果 Result 是 Ok(None)，则返回 None。
        // 如果 Result 是 Err(error)，则返回 Some(Err(error))。
        self.lexer.peek().cloned().transpose()
    }

    fn custom_next(&mut self) -> LegendDBResult<Token> {
        self.lexer.next().unwrap_or_else(|| Err(LegendDBError::Parser("[Parser] Unexpected end of input".to_string())))
    }

    fn next_ident(&mut self) -> LegendDBResult<String> {
        match self.custom_next()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(LegendDBError::Parser(format!("[Parser] Unexpected token: {:?}", token)))
        }
    }
}