use std::iter::Peekable;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::lexer::{Lexer, Token};
use crate::utils::custom_error::LegendDBResult;

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

    }
    
    fn parse_statement(&mut self) -> LegendDBResult<Statement> {
        match self.custom_peek()? {
            Some(Token::Keyword) => {
                
            }
        }
    }
    
    fn custom_peek(&mut self) -> LegendDBResult<Option<Token>> {
        //transpose 方法作用是将 Option<Result<T, E>> 转换为 Result<Option<T>, E>
        self.lexer.peek().cloned().transpose()
    }
}