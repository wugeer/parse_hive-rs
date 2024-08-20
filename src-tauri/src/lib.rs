use regex::Regex;
use sqlparser::ast::Expr::{BinaryOp, Exists, InSubquery, Subquery};
use sqlparser::ast::Join;
use sqlparser::ast::Select;
use sqlparser::ast::TableFactor::{Derived, Table};
use sqlparser::ast::{
    CreateTable, Expr, Insert, ObjectName, Query, SetExpr, Statement, TableWithJoins, With,
};
use sqlparser::dialect::HiveDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::error::Error;

#[derive(Debug)]
pub struct HiveSqlParser {
    current_database: String,
    all_table_names: Vec<String>,
    table_names: Vec<String>,
    cte_names: HashSet<String>,
}

impl Default for HiveSqlParser {
    fn default() -> Self {
        Self::new()
    }
}

impl HiveSqlParser {
    pub fn new() -> Self {
        Self {
            current_database: "default".to_string(),
            all_table_names: Vec::new(),
            table_names: Vec::new(),
            cte_names: HashSet::new(),
        }
    }

    /// 移除 Hive SQL 查询中的注释（包括单行和多行注释）。
    ///
    /// # 参数
    ///
    /// * `query` - 输入的 Hive SQL 查询字符串。
    ///
    /// # 返回值
    ///
    /// 返回一个移除了注释的 SQL 字符串。
    fn remove_hive_sql_comments(&mut self, query: &str) -> String {
        // 正则表达式匹配多行注释 (/* */)
        let multiline_comment_re = Regex::new(r"(?s)/\*.*?\*/").unwrap();
        // 正则表达式匹配单行注释 (--)，并匹配到行尾
        let singleline_comment_re = Regex::new(r"--[^\n]*").unwrap();

        // 先移除多行注释
        let without_multiline_comments = multiline_comment_re.replace_all(query, "");
        // 再移除单行注释
        let without_comments = singleline_comment_re.replace_all(&without_multiline_comments, "");

        // 移除可能留下的多余空行
        let cleaned_query = without_comments
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        cleaned_query
    }
    pub fn parse(&mut self, queries: &str) -> Result<(), Box<dyn Error>> {
        let dialect = HiveDialect {};
        let re = Regex::new(
            r"(?s)(partitioned\s+by.*)?clustered\s+by\s*\([^)]+\)\s+into\s+\d+\s+buckets",
        )
        .unwrap();
        for query in queries.split(';') {
            let query = query.trim().to_lowercase();
            let query = re.replace_all(&query, "");
            let query = self.remove_hive_sql_comments(&query);
            println!("cleaned query is:{:?}", query);
            // 忽略空行和配置行
            if query.is_empty() || query.starts_with("set ") {
                continue;
            }
            if query.starts_with("use ") {
                self.handle_use_database(&query);
            } else {
                self.handle_query(&query, &dialect)?;
                self.all_table_names.extend(
                    self.table_names
                        .drain(..)
                        .filter(|name| !self.cte_names.contains(name))
                        .collect::<Vec<_>>(),
                );
                self.cte_names.clear();
            }
        }
        Ok(())
    }

    fn handle_use_database(&mut self, query: &str) {
        let parts: Vec<&str> = query.split_whitespace().collect();
        if parts.len() == 2 {
            self.current_database = parts[1].to_string();
            println!("current_database={:?}", self.current_database);
        }
    }

    fn handle_query(&mut self, query: &str, dialect: &HiveDialect) -> Result<(), Box<dyn Error>> {
        let ast = Parser::parse_sql(dialect, query)?;
        for stmt in ast {
            println!("stmt={:?}", stmt);
            self.handle_statment(&stmt);
        }
        Ok(())
    }

    fn handle_statment_query(&mut self, query: &Query) {
        if let SetExpr::Select(select) = &*query.body {
            // 处理 FROM 子句
            for table_with_joins in &select.from {
                if let TableWithJoins {
                    relation: Table { name, .. },
                    joins,
                    ..
                } = table_with_joins
                {
                    println!("Table name: {:?}", name);
                    self.add_valid_table_name(name);
                    for j in joins {
                        match &j.relation {
                            Table { name, .. } => self.add_valid_table_name(name),
                            Derived { subquery, .. } => {
                                self.extract_table_names_from_query(subquery)
                            }
                            _ => println!("忽略分支:{:?}", &j.relation),
                        };
                    }
                }
            }
        }
    }

    fn handle_statment(&mut self, stmt: &Statement) {
        match stmt {
            // 处理 CREATE TABLE AS SELECT 语句
            Statement::CreateTable(CreateTable {
                query: Some(boxed_query),
                ..
            }) => {
                self.handle_statment_query(boxed_query);
            }

            // 处理 INSERT INTO ... SELECT 语句
            Statement::Insert(Insert {
                source: Some(boxed_source),
                ..
            }) => {
                self.handle_statment_query(boxed_source);
            }

            // 处理普通的查询语句
            Statement::Query(query) => {
                self.extract_table_names_from_query(query);
            }

            Statement::CreateView { query, .. } => {
                self.extract_table_names_from_query(query);
            }

            Statement::Directory { source, .. } => {
                self.extract_table_names_from_query(source);
            }

            _ => println!("处理statment的默认分支:{:?}", stmt),
        }
    }

    fn add_valid_table_name(&mut self, name: &ObjectName) {
        let origin_table_name = self.get_origin_table_name(name);
        if !self.cte_names.contains(&origin_table_name) {
            self.table_names.push(self.get_actual_table_name(name));
        }
    }

    fn extract_cte_names(&mut self, with: &With) {
        for cte in &with.cte_tables {
            self.cte_names.insert(cte.alias.name.to_string());
            self.extract_table_names_from_query(&cte.query);
        }
    }

    fn extract_table_names_from_joins(&mut self, joins: &Vec<Join>) {
        for join in joins {
            match &join.relation {
                Table { name, .. } => self.add_valid_table_name(name),
                Derived { subquery, .. } => self.extract_table_names_from_query(subquery),
                _ => println!("处理joins的relation的默认分支:{:?}", &join.relation),
            };
        }
    }

    fn extract_table_names_from_expr(&mut self, expr: &Expr) {
        match expr {
            Subquery(subquery) => {
                self.extract_table_names_from_query(subquery);
            }
            _ => println!("expr默认分支:{:?}", expr),
        };
    }

    fn extract_table_names_from_select(&mut self, select: &Select) {
        for table_with_joins in &select.from {
            match table_with_joins {
                TableWithJoins {
                    relation: Table { name, .. },
                    joins,
                    ..
                } => {
                    self.add_valid_table_name(name);
                    self.extract_table_names_from_joins(joins);
                }
                TableWithJoins {
                    relation: Derived { subquery, .. },
                    joins,
                    ..
                } => {
                    self.extract_table_names_from_query(subquery);
                    self.extract_table_names_from_joins(joins);
                }
                _ => println!("table_with_joins默认分支:{:?}", table_with_joins),
            };
        }
        // 处理where子查询
        match &select.selection {
            Some(Exists { subquery, .. }) | Some(InSubquery { subquery, .. }) => {
                self.extract_table_names_from_query(subquery);
            }
            Some(BinaryOp { right, left, .. }) => {
                self.extract_table_names_from_expr(right);
                self.extract_table_names_from_expr(left);
            }
            _ => {
                println!("select.selection默认分支:{:?}", select.selection);
            }
        }

        match &select.having {
            Some(BinaryOp { right, left, .. }) => {
                self.extract_table_names_from_expr(right);
                self.extract_table_names_from_expr(left);
            }
            _ => {
                println!("select.having默认分支:{:?}", select.having);
            }
        }
    }

    fn extract_table_names_from_set_option(&mut self, node: &SetExpr) {
        match node {
            SetExpr::SetOperation { left, right, .. } => {
                self.extract_table_names_from_set_option(left);
                self.extract_table_names_from_set_option(right);
            }
            SetExpr::Select(select) => self.extract_table_names_from_select(select),
            _ => println!(
                "extract_table_names_from_set_option node默认分支:{:?}",
                node
            ),
        }
    }

    fn extract_table_names_from_query(&mut self, query: &Query) {
        // 处理 WITH 子句
        if let Some(with) = &query.with {
            self.extract_cte_names(with);
        }
        match &*query.body {
            SetExpr::Select(select) => self.extract_table_names_from_select(select),
            SetExpr::Query(query) => self.extract_table_names_from_query(query),
            // 处理 INSERT INTO ... SELECT ... 语句
            SetExpr::Insert(insert) => self.handle_statment(insert),
            SetExpr::SetOperation { left, right, .. } => {
                self.extract_table_names_from_set_option(left);
                self.extract_table_names_from_set_option(right);
            }
            _ => println!(
                "extract_table_names_from_query query.body默认分支:{:?}",
                &*query.body
            ),
        }
    }

    fn get_actual_table_name(&self, name: &ObjectName) -> String {
        let name_parts = name
            .0
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>();
        if name_parts.len() == 2 {
            // 如果表名已经包含了数据库名
            name_parts.join(".")
        } else {
            // 否则加上当前的数据库名
            format!("{}.{}", self.current_database, name_parts.join("."))
        }
    }

    fn get_origin_table_name(&self, name: &ObjectName) -> String {
        name.0
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<String>()
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.all_table_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_use_between_queries() {
        let query =
            "select id, name from test.my_table where id > 10; select * from test.another_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.another_table".to_string()));
    }

    #[test]
    fn test_parse_create_table() {
        let query = "create table if not exists test.my_table(id int, name string)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
    }

    #[test]
    fn test_parse_select_query() {
        let query = "select id, name from test.my_table where id > 10";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert_eq!("test.my_table", table_names[0]);
    }

    #[test]
    fn test_parse_insert_query() {
        let query = "insert into test.my_table values (1, 'John')";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
        // Uncomment if insert queries should be handled
        // assert_eq!("test.my_table", table_names[0]);
    }

    #[test]
    fn test_parse_create_table_as_select() {
        let query = "create table test.new_table as select id, name from test.my_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_with_clause() {
        let query =
            "with cte_table as (select id, name from test.my_table) select * from cte_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_insert_overwrite_query() {
        let query = "insert overwrite table test.my_table select id, name from test.another_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert_eq!("test.another_table", table_names[0]);
    }

    #[test]
    fn test_parse_insert_overwrite_query_1() {
        let query = "-- set config test\n\
                SET hive.exec.dynamic.partition.mode=nonstrict; -- hello comment select * from test.aaaa\n\
                -- select * from test.bbbb\n\
                \n\
                with temp_a as (select * from test.table_1 where id=1)insert overwrite table test.my_table select id, name from test.another_table a join temp_a b on a.id=b.id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.another_table".to_string()));
        assert!(table_names.contains(&"test.table_1".to_string()));
    }

    #[test]
    fn test_parse_subquery() {
        let query = "SET hivevar:min_price=100;\n\
                -- nothing to do: select * from test.bbbb\n\
                \n\
                select id, name from (select id, name from test.my_table) t where t.id > 10";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert_eq!("test.my_table", table_names[0]);
    }

    #[test]
    fn test_parse_join_query() {
        let query =
            "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_parse_join_query_1() {
        let query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(4, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
        assert!(table_names.contains(&"test.table9".to_string()));
    }

    #[test]
    fn test_parse_join_query_3() {
        let query = "with temp_a as (select * from test.table5), temp_b as (select * from test.table6), temp_c as (select * from temp_a join temp_b on temp_a.id=temp_b.id)select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name right join temp_c t4 on t1.id=t4.id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(6, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
        assert!(table_names.contains(&"test.table9".to_string()));
        assert!(table_names.contains(&"test.table5".to_string()));
        assert!(table_names.contains(&"test.table6".to_string()));
    }

    #[test]
    fn test_parse_exists_query() {
        let query = "select id, name from test.my_table where exists (select 1 from test.another_table where another_table.id = my_table.id)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.another_table".to_string()));
    }

    #[test]
    fn test_parse_in_query() {
        let query =
            "select id, name from test.my_table where id in (select id from test.filter_table)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.filter_table".to_string()));
    }

    #[test]
    fn test_parse_group_by_query() {
        let query = "select id, count(*) from test.my_table group by id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_order_by_query() {
        let query = "select id, name from test.my_table order by id desc";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_having_query() {
        let query = "select id, count(*) as cnt from test.my_table group by id having cnt > 5";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_distinct_query() {
        let query = "select distinct id from test.my_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_parse_subquery_in_where() {
        let query = "select id, name from test.my_table where id = (select max(id) from test.another_table)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.another_table".to_string()));
    }

    #[test]
    fn test_parse_multiple_statements() {
        let query = "insert overwrite table test.table1 select id, name from test.another_table1; insert overwrite table test.table2 select id, name from test.another_table2";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.another_table1".to_string()));
        assert!(table_names.contains(&"test.another_table2".to_string()));
    }
    #[test]
    fn test_use_with_insert_into() {
        let query = "use test_db; insert into test.my_table values (1, 'John')";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
        // assert_eq!("test.my_table", table_names[0]);
    }

    #[test]
    fn test_use_with_create_table_as_select() {
        let query =
            "use test_db; create table test.new_table as select id, name from test.my_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_with_with_clause() {
        let query = "use test_db; with cte_table as (select id, name from test.my_table) select * from cte_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_with_insert_overwrite() {
        let query = "use test_db; insert overwrite table test.my_table select id, name from test.another_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert_eq!("test.another_table", table_names[0]);
    }

    #[test]
    fn test_use_with_subquery() {
        let query = "use test_db; select id, name from (select id, name from test.my_table) t where t.id > 10";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert_eq!("test.my_table", table_names[0]);
    }

    #[test]
    fn test_use_with_join_query() {
        let query = "use test_db; select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_use_with_complex_join_query() {
        let query = "use test_db; select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(4, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
        assert!(table_names.contains(&"test.table9".to_string()));
    }

    #[test]
    fn test_use_with_exists_query() {
        let query = "use test_db; select id, name from test.my_table where exists (select 1 from test.another_table where another_table.id = my_table.id)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.another_table".to_string()));
    }

    #[test]
    fn test_use_with_in_query() {
        let query = "use test_db; select id, name from test.my_table where id in (select id from test.filter_table)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test.filter_table".to_string()));
    }

    #[test]
    fn test_use_with_group_by_query() {
        let query = "use test_db; select id, count(*) from test.my_table group by id";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_with_order_by_query() {
        let query = "use test_db; select id, name from test.my_table order by id desc";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_with_having_query() {
        let query =
            "use test_db; select id, count(*) as cnt from test.my_table group by id having cnt > 5";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_with_partitioned_table() {
        let query = "use test_db; CREATE TABLE test.partitioned_table (id INT, name STRING) PARTITIONED BY (dt STRING)";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
    }

    #[test]
    fn test_use_with_insert_into_partition() {
        let query = "use test_db; INSERT INTO TABLE test.partitioned_table PARTITION (dt='2023-05-01') SELECT id, name FROM test.source_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.source_table".to_string()));
    }

    #[test]
    fn test_use_with_create_external_table() {
        let query = "use test_db; CREATE EXTERNAL TABLE test.external_table (id INT, name STRING) STORED AS PARQUET LOCATION '/path/to/data'";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
    }

    #[test]
    fn test_use_with_bucketed_table() {
        let query = r#"
CREATE TABLE user_info_bucketed(user_id BIGINT, firstname STRING, lastname STRING)
COMMENT 'A bucketed copy of user_info'
PARTITIONED BY(ds STRING)
CLUSTERED BY(user_id) INTO 256 BUCKETS;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(0, table_names.len());
    }

    #[test]
    fn test_use_with_window_function() {
        let query = "use test_db; SELECT id, name, AVG(salary) OVER (PARTITION BY department ORDER BY salary) AS avg_salary FROM test.employee_table";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.employee_table".to_string()));
    }

    #[test]
    fn test_use_with_lateral_view_explode() {
        let query = "use test_db; SELECT t.id, t.name, item FROM test.table_with_array t LATERAL VIEW EXPLODE(t.items) itemTable AS item";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(1, table_names.len());
        assert!(table_names.contains(&"test.table_with_array".to_string()));
    }

    #[test]
    fn test_use_with_complex_union_query() {
        let query = "SET hive.exec.parallel=true;\n\
                     use test_db; SELECT id, name FROM test.table1 UNION ALL SELECT id, name FROM test.table2 UNION SELECT id, name FROM test.table3";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(3, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
    }

    #[test]
    fn test_use_with_union_in_cte() {
        let query = "use test_db; WITH cte AS (SELECT id, name FROM test.table1 UNION ALL SELECT id, name FROM test.table2) SELECT * FROM cte";
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(2, table_names.len());
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_multi_line_comment() {
        let query = r#"/* This is a multi-line comment
                       spanning multiple lines */
                       SELECT * FROM test.table1;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_comment_within_join_clause() {
        let query = r#"SELECT t1.*, t2.* FROM test.table1 t1
                       JOIN /* This is a comment within a JOIN clause */ test.table2 t2
                       ON t1.id = t2.id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_comment_within_subquery() {
        let query = r#"SELECT * FROM (
                           SELECT id, name -- This is a comment within a subquery
                           FROM test.table1
                           WHERE id > 10
                       ) subquery;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_comment_within_cte() {
        let query = r#"WITH cte AS (
                           -- This is a comment within a CTE
                           SELECT id, name FROM test.table1
                       )
                       SELECT * FROM cte;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_comment_within_insert_statement() {
        let query = r#"INSERT INTO test.table2
                       -- This is a comment within an INSERT statement
                       SELECT * FROM test.table1;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_multiple_comments_in_complex_query() {
        let query = r#"-- Initial comment
                       WITH cte AS (
                           SELECT id, name -- Comment in CTE
                           FROM test.table1
                       )
                       SELECT c.*, t2.* -- Comment in main query
                       FROM cte c
                       JOIN /* Multi-line comment
                               in JOIN clause */ test.table2 t2
                       ON c.id = t2.id
                       WHERE c.id > 10; -- Final comment"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_window_function_with_partition_and_order_by() {
        let query = r#"-- Calculate running total of sales for each store
                       USE sales_db;
                       SELECT store_id, sale_date, sales_amount,
                              SUM(sales_amount) OVER (PARTITION BY store_id ORDER BY sale_date) AS running_total
                       FROM sales_table;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"sales_db.sales_table".to_string()));
    }

    #[test]
    fn test_window_function_with_rows_between() {
        let query = r#"USE inventory_db;
                       -- Calculate 7-day moving average of inventory levels
                       SELECT product_id, `date`, inventory_level,
                              AVG(inventory_level) OVER (
                                  PARTITION BY product_id
                                  ORDER BY `date`
                                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                              ) AS moving_average
                       FROM inventory_table;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"inventory_db.inventory_table".to_string()));
    }

    #[test]
    fn test_multiple_window_functions_with_different_partitions() {
        let query = r#"-- Calculate rank and dense rank within each department and overall
                       USE hr_db;
                       SELECT employee_id, department, salary,
                              RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
                              DENSE_RANK() OVER (ORDER BY salary DESC) AS overall_dense_rank
                       FROM employee_table;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"hr_db.employee_table".to_string()));
    }

    #[test]
    fn test_window_function_in_subquery() {
        let query = r#"USE sales_db;
                       -- Find top 5 products by sales in each category
                       SELECT * FROM (
                           SELECT product_id, category, total_sales,
                                  DENSE_RANK() OVER (PARTITION BY category ORDER BY total_sales DESC) AS sales_rank
                           FROM (
                               -- Calculate total sales for each product
                               SELECT product_id, category, SUM(sales_amount) AS total_sales
                               FROM sales_table
                               GROUP BY product_id, category
                           ) t
                       ) t2
                       WHERE sales_rank <= 5;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"sales_db.sales_table".to_string()));
    }

    #[test]
    fn test_window_function_with_join() {
        let query = r#"-- Calculate employee's salary percentile within their department
                       USE hr_db;
                       SELECT e.employee_id, e.department, e.salary, d.dept_name,
                              CUME_DIST() OVER (PARTITION BY e.department ORDER BY e.salary) AS salary_percentile
                       FROM employee_table e
                       JOIN test.department_table d ON e.department = d.dept_id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"hr_db.employee_table".to_string()));
        assert!(table_names.contains(&"test.department_table".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_union() {
        let query = r#"-- Select from table in db1
                       USE db1;
                       SELECT * FROM table1
                       UNION ALL
                       -- Select from table in db2
                       SELECT * FROM db2.table2;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_join() {
        let query = r#"-- Join tables from different databases
                       USE db1;
                       SELECT t1.*, t2.*
                       FROM table1 t1
                       JOIN db2.table2 t2 ON t1.id = t2.id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_in_cte() {
        let query = r#"-- CTE with tables from different databases
                      USE db1;
                       WITH cte1 AS (
                           SELECT * FROM table1
                       ),
                       cte2 AS (
                           SELECT * FROM db2.table2
                       )
                       SELECT * FROM cte1
                       UNION ALL
                       SELECT * FROM cte2;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_comment_before_and_after_sql() {
        let query = r#"-- This is a comment before the SQL
                       SELECT * FROM test.table1;
                       -- This is a comment after the SQL"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_comment_between_sql_statements() {
        let query = r#"SELECT * FROM test.table1;
                       -- This is a comment between SQL statements
                       SELECT * FROM test.table2;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
    }

    #[test]
    fn test_comment_within_sql() {
        let query = r#"SELECT * FROM test.table1 -- This is an inline comment
                       WHERE id > 10;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table1".to_string()));
    }

    #[test]
    fn test_multiple_statements_with_comment() {
        let query = r#"set tez.queue.name=root.test; -- set queue name 
                       -- First, let's select from test.my_table
                       select * from test.my_table;
                       /* Now, let's switch to another database
                       select * from 
                       xhw.test where exists (select id from txs.good)  
                       hello world */
                       use test_db;
                       -- Finally, let's select from test.another_table1
                       SET hive.exec.dynamic.partition=true;
                       SET hive.exec.dynamic.partition.mode=nonstrict;
                       select * from another_table
                       -- Finally, let's select from test.another_table1;
                       SET mapreduce.job.name='My Sales Report';"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test.my_table".to_string()));
        assert!(table_names.contains(&"test_db.another_table".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_case_statement() {
        let query = r#"SET hive.fetch.task.conversion=more;
                       USE sales_db;
                       SELECT s.product_id,
                              CASE
                                  WHEN p.category = 'Electronics' THEN 'Tech'
                                  WHEN p.category = 'Clothing' THEN 'Apparel'
                                  ELSE 'Other'
                              END AS category_group,
                              SUM(s.amount) AS total_sales
                       FROM sales_table s
                       JOIN product_db.product_table p ON s.product_id = p.id
                       GROUP BY s.product_id, category_group;
                       SET hive.exec.dynamic.partition=true;
                       SET hive.exec.dynamic.partition.mode=nonstrict;
                       USE db1;
                       SELECT *
                       FROM table1 t1
                       WHERE EXISTS (
                         -- Subquery from another database
                         SELECT 1 FROM db2.table2 t2 WHERE t2.id = t1.id
                       );
                       use hello;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 4);
        assert!(table_names.contains(&"sales_db.sales_table".to_string()));
        assert!(table_names.contains(&"product_db.product_table".to_string()));
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_cte() {
        let query = r#"-- Define CTE in one database
                       USE db1;
                       WITH cte AS (
                         SELECT * FROM table1
                       )
                       SELECT * FROM cte c
                       JOIN db2.table2 t ON c.id = t.id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_group_by_and_having() {
        let query = r#"USE sales_db;
                       SELECT product, SUM(amount) AS total_sales
                       FROM sales_table
                       GROUP BY product
                       HAVING SUM(amount) > (
                         SELECT AVG(quantity * price) FROM inventory_db.inventory_table
                       );"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"sales_db.sales_table".to_string()));
        assert!(table_names.contains(&"inventory_db.inventory_table".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_lateral_view() {
        let query = r#"-- Select from table in db1
                       USE db1;
                       SELECT t.id, t.col1, lv.col2
                       FROM table1 t
                        -- Join with table from db2
                       JOIN db2.table2 t2 ON t.id = t2.id
                       LATERAL VIEW explode(split(t.col2, ',')) lv AS col2;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_subquery() {
        let query = r#"USE db1;
                       SELECT *
                       FROM table1
                       WHERE id IN (
                         -- Subquery from another database
                         SELECT id FROM db2.table2
                       );"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_union_and_join() {
        let query = r#"-- Union tables from different databases
                       USE db1;
                       SELECT * FROM table1
                       UNION ALL
                       SELECT * FROM db2.table2
                       -- Join with table from another database
                       JOIN db3.table3 ON db1.table1.id = db3.table3.id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 3);
        assert!(table_names.contains(&"db1.table1".to_string()));
        assert!(table_names.contains(&"db2.table2".to_string()));
        assert!(table_names.contains(&"db3.table3".to_string()));
    }

    #[test]
    fn test_multiple_uses_with_window_function() {
        let query = r#"-- Calculate running total sales
                       USE sales_db;
                       SELECT s.`date`, p.name, SUM(s.amount) OVER (ORDER BY s.`date`) AS running_total
                       FROM sales_table s
                       -- Join with product info from another database
                       JOIN product_db.product_table p ON s.product_id = p.id;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"sales_db.sales_table".to_string()));
        assert!(table_names.contains(&"product_db.product_table".to_string()));
    }

    #[test]
    fn test_parse_bucketed_table() {
        let query = r#"CREATE TABLE test.bucketed_table (id INT, name STRING) CLUSTERED BY (id) INTO 4 BUCKETS"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 0);
    }

    #[test]
    fn test_parse_complex_join_with_subqueries() {
        let query = r#"SELECT a.id, b.name, c.value FROM 
                       (SELECT id FROM test.table1 WHERE id > 100) a 
                       JOIN test.table2 b ON a.id = b.id 
                       LEFT JOIN (SELECT id, MAX(value) as value FROM test.table3 GROUP BY id) c ON a.id = c.id"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 3);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
    }

    #[test]
    fn test_parse_complex_union_query() {
        let query = r#"SELECT id, name FROM test.table1 UNION ALL SELECT id, name FROM test.table2 UNION SELECT id, name FROM test.table3"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 3);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
    }

    #[test]
    fn test_parse_create_external_table() {
        let query = r#"CREATE EXTERNAL TABLE test.external_table (id INT, name STRING) STORED AS PARQUET LOCATION '/path/to/data'"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 0);
    }

    #[test]
    fn test_parse_create_view_query() {
        let query =
            r#"CREATE VIEW test.my_view AS SELECT id, name FROM test.base_table WHERE id > 100"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.base_table".to_string()));
    }

    #[test]
    fn test_parse_insert_into_partition() {
        let query = r#"INSERT INTO TABLE test.partitioned_table PARTITION (dt='2023-05-01') SELECT id, name FROM test.source_table"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.source_table".to_string()));
    }

    #[test]
    fn test_parse_insert_overwrite_directory() {
        let query =
            r#"INSERT OVERWRITE DIRECTORY '/output/path' SELECT id, name FROM test.source_table"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.source_table".to_string()));
    }

    #[test]
    fn test_parse_lateral_view_explode() {
        let query = r#"SELECT t.id, t.name, item FROM test.table_with_array t LATERAL VIEW EXPLODE(t.items) itemTable AS item"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.table_with_array".to_string()));
    }

    #[test]
    fn test_parse_partitioned_table() {
        let query = r#"CREATE TABLE test.partitioned_table (id INT, name STRING) PARTITIONED BY (dt STRING)"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 0);
    }

    #[test]
    fn test_parse_window_function() {
        let query = r#"SELECT id, name, AVG(salary) OVER (PARTITION BY department ORDER BY salary) AS avg_salary FROM test.employee_table"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.employee_table".to_string()));
    }

    #[test]
    fn test_select_with_comment() {
        let query = r#"select id, name from test.my_table -- Select id, name from xxx.test \nwhere id > 10 -- Only records with id greater than 10"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.my_table".to_string()));
    }

    #[test]
    fn test_use_after_select_query() {
        let query = r#"select id, name from test.my_table where id > 10; use test_db"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert_eq!(table_names[0], "test.my_table".to_string());
    }

    #[test]
    fn test_use_before_select_query() {
        let query = r#"use test_db; select id, name from test.my_table where id > 10"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert_eq!(table_names[0], "test.my_table".to_string());
    }

    #[test]
    fn test_use_with_complex_join_with_subqueries() {
        let query = r#"SET hive.exec.compress.output=true;
                       SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
                       SELECT id, name FROM test.table_1 UNION ALL SELECT id, name FROM table_2 UNION SELECT id, name FROM test.table_3;
                       use test_db; SELECT a.id, b.name, c.value FROM 
                       (SELECT id FROM test.table1 WHERE id > 100) a 
                       JOIN table2 b ON a.id = b.id 
                       LEFT JOIN (SELECT id, MAX(value) as value FROM test.table3 GROUP BY id) c ON a.id = c.id"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 6);
        assert!(table_names.contains(&"test.table1".to_string()));
        assert!(table_names.contains(&"test.table_1".to_string()));
        assert!(table_names.contains(&"default.table_2".to_string()));
        assert!(table_names.contains(&"test_db.table2".to_string()));
        assert!(table_names.contains(&"test.table3".to_string()));
        assert!(table_names.contains(&"test.table_3".to_string()));
    }

    #[test]
    fn test_use_with_create_table() {
        let query = r#"-- ok to test: select 1 from tst.xxx
                       SET hive.enforce.bucketing=true; --- enforce buctket to true 
                       CREATE TABLE users_bucketed (
                         user_id INT, -- to comment 
                         name STRING comment 'ddddd',
                         age INT
                       )
                       CLUSTERED BY (user_id) INTO 4 BUCKETS;"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 0);
    }

    #[test]
    fn test_use_with_create_view_query() {
        let query = r#"use test_db; CREATE VIEW test.my_view AS SELECT id, name FROM test.base_table WHERE id > 100"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.base_table".to_string()));
    }

    #[test]
    fn test_use_with_insert_overwrite_directory() {
        let query = r#"-- test set queue name and mutil line comment  select * from test.111111 
                       set tez.queue.name=root.test; 
                       use test_db; INSERT OVERWRITE DIRECTORY '/output/path' SELECT id, name FROM test.source_table"#;
        let mut processor = HiveSqlParser::new();
        processor.parse(query).unwrap();
        let table_names = processor.get_table_names();
        assert_eq!(table_names.len(), 1);
        assert!(table_names.contains(&"test.source_table".to_string()));
    }
}
