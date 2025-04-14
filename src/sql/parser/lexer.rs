// 词法分析 Lexer 定义
// 目前支持的 SQL 语法

use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::str::Chars;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Database,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Where,
    Insert,
    Update,
    Set,
    Delete,
    Alter,
    Show,
    Drop,
    Into,
    Values,
    True,
    False,
    Default,
    If,
    Not,
    Null,
    Exists,
    Primary,
    Key,
    And,
    Or,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    As,
    Cross,
    Join,
    Left,
    Right,
    On
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        match ident.to_uppercase().as_ref() {
            "CREATE" => Some(Keyword::Create),
            "DATABASE" => Some(Keyword::Database),
            "TABLE" => Some(Keyword::Table),
            "INT" => Some(Keyword::Int),
            "INTEGER" => Some(Keyword::Integer),
            "BOOLEAN" => Some(Keyword::Boolean),
            "BOOL" => Some(Keyword::Bool),
            "STRING" => Some(Keyword::String),
            "TEXT" => Some(Keyword::Text),
            "VARCHAR" => Some(Keyword::Varchar),
            "DOUBLE" => Some(Keyword::Double),
            "FLOAT" => Some(Keyword::Float),
            "SELECT" => Some(Keyword::Select),
            "UPDATE" => Some(Keyword::Update),
            "SET" => Some(Keyword::Set),
            "DELETE" => Some(Keyword::Delete),
            "ALTER" => Some(Keyword::Alter),
            "SHOW" => Some(Keyword::Show),
            "DROP" => Some(Keyword::Drop),
            "FROM" => Some(Keyword::From),
            "WHERE" => Some(Keyword::Where),
            "INSERT" => Some(Keyword::Insert),
            "INTO" => Some(Keyword::Into),
            "VALUES" => Some(Keyword::Values),
            "TRUE" => Some(Keyword::True),
            "FALSE" => Some(Keyword::False),
            "PRIMARY" => Some(Keyword::Primary),
            "KEY" => Some(Keyword::Key),
            "NULL" => Some(Keyword::Null),
            "DEFAULT" => Some(Keyword::Default),
            "IF" => Some(Keyword::If),
            "NOT" => Some(Keyword::Not),
            "EXISTS" => Some(Keyword::Exists),
            "AND" => Some(Keyword::And),
            "OR" => Some(Keyword::Or),
            "ORDER" => Some(Keyword::Order),
            "BY" => Some(Keyword::By),
            "ASC" => Some(Keyword::Asc),
            "DESC" => Some(Keyword::Desc),
            "LIMIT" => Some(Keyword::Limit),
            "OFFSET" => Some(Keyword::Offset),
            "AS" => Some(Keyword::As),
            "CROSS" => Some(Keyword::Cross),
            "JOIN" => Some(Keyword::Join),
            "LEFT" => Some(Keyword::Left),
            "RIGHT" => Some(Keyword::Right),
            "ON" => Some(Keyword::On),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Database => "DATABASE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Delete => "DELETE",
            Keyword::Alter => "ALTER",
            Keyword::Show => "SHOW",
            Keyword::Drop => "DROP",
            Keyword::From => "FROM",
            Keyword::Where => "WHERE",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
            Keyword::Null => "NULL",
            Keyword::Default => "DEFAULT",
            Keyword::If => "IF",
            Keyword::Not => "NOT",
            Keyword::Exists => "EXISTS",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::As => "AS",
            Keyword::Cross => "CROSS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::On => "ON",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}
#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub enum Token {
    // 关键字
    Keyword(Keyword),
    // 标识符
    Identifier(String),
    // 数字
    Number(String),
    // 字符串
    String(String),
    // 左括号
    LeftParen,
    // 右括号
    RightParen,
    // 左中括号
    LeftBracket,
    // 右中括号
    RightBracket,
    // 左大括号
    LeftBrace,
    // 右大括号
    RightBrace,
    // 点号
    Dot,
    // 逗号
    Comma,
    // 分号
    Semicolon,
    // 星号
    Star,
    // 加好
    Plus,
    // 减号
    Minus,
    // 乘号
    Asterisk,
    // 除号
    Slash,
    // 冒号
    Colon,
    // 等号
    Equal,
    // 大于号
    GreaterThan,
    // 小于号
    LessThan,
    // 等于号
    DoubleEqual,
    // 不等于号
    NotEqual,
    // 空白
    Whitespace,
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Identifier(ident) => ident,
            Token::Number(num) => num,
            Token::String(string) => string,
            Token::LeftParen => "(",
            Token::RightParen => ")",
            Token::LeftBracket => "[",
            Token::RightBracket => "]",
            Token::LeftBrace => "{",
            Token::RightBrace => "}",
            Token::Dot => ".",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Star => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Slash => "/",
            Token::Colon => ":",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
            Token::DoubleEqual => "==",
            Token::NotEqual => "!=",
            Token::Whitespace => " ",
        })
    }
}
// 1. Create Table
// -------------------------------------
// CREATE TABLE table_name (
//     [ column_name data_type [ column_constraint [...] ] ]
//     [, ... ]
//    );
//
//    where data_type is:
//     - BOOLEAN(BOOL): true | false
//     - FLOAT(DOUBLE)
//     - INTEGER(INT)
//     - STRING(TEXT, VARCHAR)
//
//    where column_constraint is:
//    [ NOT NULL | NULL | DEFAULT expr ]
//
// 2. Insert Into
// -------------------------------------
// INSERT INTO table_name
// [ ( column_name [, ...] ) ]
// values ( expr [, ...] );
// 3. Select * From
// -------------------------------------
// SELECT * FROM table_name;
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
    prev_token: Option<Token>,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = LegendDBResult<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => {Some(Ok(token))},
            Ok(None) => {self.iter.peek().map(|_| Err(LegendDBError::NotSupported))},
            Err(e) => {Some(Err(e))},
        }
    }
}

impl<'a> Lexer<'a> {

    pub fn new(sql: &'a str) -> Lexer<'a> {
        Lexer {
            iter: sql.chars().peekable(),
            prev_token: None,
        }
    }

    /// 消除空白字符
    /// ex： select    *     from   table

    fn skip_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    /// 判断当前字符是否满足条件，如果是空白字符则跳到下一个字符
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate){
            value.push(c);
        }
        Some(value).filter(|s| !s.is_empty())
    }
    /// 判断当前字符是否满足条件，只有Token类型才跳转到下一个，并返回Token类型
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| {predicate(*c)})?;
        self.iter.next();
        Some(token)
    }

    // 词法分析
    pub fn scan(&mut self) -> LegendDBResult<Option<Token>> {
        //清除字符串中空白部分
        self.skip_whitespace();
        // 根据第一个字符判断
        match self.iter.peek() {
            Some('\'') => self.scan_string(), // 扫描字符串
            // is_ascii_digit 判断是否是数字
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // 扫描数字
            // is_alphabetic 判断是否是字母
            Some(c) if c.is_alphabetic() => Ok(self.scan_identifier()), // 扫描ident 类型
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }.map(|token| {
            if let Some(t) = &token {
                self.prev_token = Some(t.clone()); // 更新上一个 Token
            }
            token
        })
    }

    /// 扫描字符串是否是单引号
    fn scan_string(&mut self) -> LegendDBResult<Option<Token>> {
        // 扫描字符串结束
        if self.next_if(|c| c == '\'' || c == '\"').is_none() {
            return Ok(None);
        }
        // 扫描字符串
        let mut value = String::new();
        // 扫描字符串
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => value.push(c),
                None => return Err(LegendDBError::NotSupported)
            }
        }
        Ok(Some(Token::String(value)))
    }

    /// 扫描数字
    fn scan_number(&mut self) -> Option<Token> {
        // 先扫描一部分
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        // 如果中间存在小数点，说明是浮点数
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // 扫描小数点之后的部分
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }
        Some(Token::Number(num))
    }

    // 扫描identifier类型，比如表名，字段名
    fn scan_identifier(&mut self) -> Option<Token> {
        // 表明，字段名必须是字母或者下划线
        let mut value = self.next_if(|c| c.is_ascii_alphanumeric() || c == '_')?.to_string();
        // 扫描表名
        while let Some(c) = self.next_if(|c| c.is_ascii_alphanumeric() || c == '_') {
                value.push(c);
            }
        Some(Keyword::from_str(&value).map_or(Token::Identifier(value.to_lowercase()), Token::Keyword))
    }

    //扫描符号
    fn scan_symbol(&mut self) -> Option<Token> {
        // cannot borrow `*self` as mutable because it is also borrowed as immutable [E0502] mutable borrow occurs here
        // Rust 不允许在同一作用域内同时存在不可变借用和可变借用，  self.prev_token（不可变借用）和 self.next_if_token（可变借用），提前获取上一个Token，不然会报不可变
        let prev_token = self.prev_token.clone();
        self.next_if_token(|c| match c {
            '*' => {
                if prev_token == Some(Token::Keyword(Keyword::Select)) {
                    Some(Token::Star)
                } else {
                    Some(Token::Asterisk)
                }
            },
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            ':' => Some(Token::Colon),
            '=' => Some(Token::Equal),
            '>' => Some(Token::GreaterThan),
            '<' => Some(Token::LessThan),
            '!' => Some(Token::NotEqual),
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '.' => Some(Token::Dot),
            '[' => Some(Token::LeftBracket),
            ']' => Some(Token::RightBracket),
            '{' => Some(Token::LeftBrace),
            '}' => Some(Token::RightBrace),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        sql::parser::lexer::{Keyword, Token},
    };
    use crate::utils::custom_error::LegendDBResult;

    #[test]
    fn test_lexer_create_table() -> LegendDBResult<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
                (
                    id1 int primary key,
                    id2 integer default 100
                );
                ",
        )
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Identifier("tbl".to_string()),
                Token::LeftParen,
                Token::Identifier("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Identifier("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::Keyword(Keyword::Default),
                Token::Number(100.to_string()),
                Token::RightParen,
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
                        (
                            id1 int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );
                        ",
        )
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;

        assert!(tokens2.len() > 0);

        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> LegendDBResult<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Identifier("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::LeftParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::RightParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Identifier("tbl".to_string()),
                Token::LeftParen,
                Token::Identifier("id".to_string()),
                Token::Comma,
                Token::Identifier("name".to_string()),
                Token::Comma,
                Token::Identifier("age".to_string()),
                Token::RightParen,
                Token::Keyword(Keyword::Values),
                Token::LeftParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::RightParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select() -> LegendDBResult<()> {
        let tokens1 = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;
        println!("{:?}", tokens1.clone());
        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Star,
                Token::Keyword(Keyword::From),
                Token::Identifier("tbl".to_string()),
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_update() -> LegendDBResult<()> {
        let tokens1 = Lexer::new("update tb1 set a = 1, b = 2 where c=2 and d=4;")
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;
        println!("{:?}", tokens1.clone());
        Ok(())
    }
    #[test]
    fn test_lexer_delete() -> LegendDBResult<()> {
        let tokens1 = Lexer::new("delete tb1 where c=2;")
            .peekable()
            .collect::<LegendDBResult<Vec<_>>>()?;
        println!("{:?}", tokens1.clone());
        assert_eq!(tokens1, 
        vec![
            Token::Keyword(Keyword::Delete),
            Token::Identifier("tb1".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("c".to_string()),
            Token::Equal,
            Token::Number("2".to_string()),
            Token::Semicolon,
        ]);
        Ok(())
    }
    
}