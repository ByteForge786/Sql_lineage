 import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison, Parenthesis
from typing import List, Dict, Set, Tuple

class SQLLineage:
    def __init__(self):
        self.lineage = {}
        self.cte_definitions = {}
        self.subquery_aliases = {}

    def parse_query(self, query: str) -> Dict[str, Set[str]]:
        statements = sqlparse.parse(query)
        for statement in statements:
            self._analyze_statement(statement)
        return self.lineage

    def _analyze_statement(self, statement, parent_alias=None):
        if statement.get_type() == 'SELECT':
            return self._analyze_select(statement, parent_alias)
        elif statement.get_type() == 'INSERT':
            return self._analyze_insert(statement)
        elif statement.get_type() == 'UPDATE':
            return self._analyze_update(statement)
        elif statement.get_type() == 'DELETE':
            return self._analyze_delete(statement)
        elif statement.get_type() == 'CREATE':
            return self._analyze_create(statement)

    def _analyze_select(self, statement, parent_alias=None):
        ctes = self._extract_ctes(statement)
        for cte_name, cte_query in ctes:
            self.cte_definitions[cte_name] = cte_query
            self._analyze_statement(cte_query, cte_name)

        select_items = self._extract_select_items(statement)
        from_tables, subqueries = self._extract_from_tables_and_subqueries(statement)
        where_columns = self._extract_where_columns(statement)
        join_columns = self._extract_join_columns(statement)

        for subq_alias, subq in subqueries:
            self.subquery_aliases[subq_alias] = self._analyze_statement(subq, subq_alias)

        for item in select_items:
            target = f"{parent_alias}.{item}" if parent_alias else item
            self.lineage[target] = set()
            for table in from_tables:
                self.lineage[target].add(f"{table}.{item.split('.')[-1]}")
            for subq_alias in self.subquery_aliases:
                if item in self.subquery_aliases[subq_alias]:
                    self.lineage[target].update(self.subquery_aliases[subq_alias][item])
            self.lineage[target].update(where_columns)
            self.lineage[target].update(join_columns)

        return self.lineage

    def _analyze_insert(self, statement):
        target_table = self._extract_insert_target(statement)
        source_query = self._extract_insert_source(statement)
        self._analyze_statement(source_query)
        
        for col, sources in self.lineage.items():
            self.lineage[f"{target_table}.{col}"] = sources

    def _analyze_update(self, statement):
        target_table = self._extract_update_target(statement)
        set_items = self._extract_set_items(statement)
        where_columns = self._extract_where_columns(statement)

        for item in set_items:
            self.lineage[f"{target_table}.{item}"] = set(where_columns)

    def _analyze_delete(self, statement):
        target_table = self._extract_delete_target(statement)
        where_columns = self._extract_where_columns(statement)

        for col in where_columns:
            self.lineage[f"{target_table}.{col}"] = set(where_columns)

    def _analyze_create(self, statement):
        table_name = self._extract_create_table_name(statement)
        columns = self._extract_create_columns(statement)
        
        for col in columns:
            self.lineage[f"{table_name}.{col}"] = set()

    def _extract_ctes(self, statement) -> List[Tuple[str, sqlparse.sql.Statement]]:
        ctes = []
        with_token = next((token for token in statement.tokens if token.is_keyword and token.normalized == 'WITH'), None)
        if with_token:
            cte_list = with_token.parent
            for token in cte_list.tokens:
                if isinstance(token, Identifier):
                    cte_name = token.get_real_name()
                    cte_query = token.tokens[-1]
                    ctes.append((cte_name, cte_query))
        return ctes

    def _extract_select_items(self, statement) -> List[str]:
        select_items = []
        select_token = next(token for token in statement.tokens if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT')
        select_idx = statement.token_index(select_token)
        identifier_list = statement.tokens[select_idx + 1]
        if isinstance(identifier_list, IdentifierList):
            for identifier in identifier_list.get_identifiers():
                select_items.append(str(identifier))
        elif isinstance(identifier_list, Identifier):
            select_items.append(str(identifier_list))
        return select_items

    def _extract_from_tables_and_subqueries(self, statement) -> Tuple[List[str], List[Tuple[str, sqlparse.sql.Statement]]]:
        from_seen = False
        tables = []
        subqueries = []
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    tables.append(token.get_real_name())
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, Identifier):
                            tables.append(identifier.get_real_name())
                        elif isinstance(identifier, Parenthesis):
                            alias = self._get_alias(identifier)
                            subqueries.append((alias, identifier.tokens[1]))
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_seen = True
        return tables, subqueries

    def _get_alias(self, token):
        alias = None
        for t in token.tokens:
            if t.ttype is sqlparse.tokens.Keyword and t.value.upper() == 'AS':
                alias = str(token.tokens[token.tokens.index(t) + 1])
        return alias or f"subquery_{len(self.subquery_aliases)}"

    def _extract_where_columns(self, statement) -> Set[str]:
        columns = set()
        where_clause = next((token for token in statement.tokens if isinstance(token, Where)), None)
        if where_clause:
            columns.update(self._extract_columns_from_token(where_clause))
        return columns

    def _extract_join_columns(self, statement) -> Set[str]:
        columns = set()
        join_seen = False
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.Keyword and 'JOIN' in token.value.upper():
                join_seen = True
            elif join_seen:
                columns.update(self._extract_columns_from_token(token))
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('WHERE', 'GROUP', 'HAVING', 'ORDER'):
                    break
        return columns

    def _extract_columns_from_token(self, token) -> Set[str]:
        columns = set()
        if isinstance(token, Comparison):
            columns.add(str(token.left))
            columns.add(str(token.right))
        elif isinstance(token, Function):
            for t in token.tokens:
                if isinstance(t, Identifier):
                    columns.add(str(t))
        elif isinstance(token, Identifier):
            columns.add(str(token))
        elif hasattr(token, 'tokens'):
            for t in token.tokens:
                columns.update(self._extract_columns_from_token(t))
        return columns

    def _extract_insert_target(self, statement) -> str:
        insert_token = next(token for token in statement.tokens if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'INSERT')
        into_token = statement.token_next_by_instance(0, Identifier)
        return into_token.get_real_name()

    def _extract_insert_source(self, statement) -> sqlparse.sql.Statement:
        select_token = next(token for token in statement.tokens if isinstance(token, sqlparse.sql.Statement) and token.get_type() == 'SELECT')
        return select_token

    def _extract_update_target(self, statement) -> str:
        update_token = next(token for token in statement.tokens if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'UPDATE')
        target_token = statement.token_next_by_instance(0, Identifier)
        return target_token.get_real_name()

    def _extract_set_items(self, statement) -> List[str]:
        set_items = []
        set_seen = False
        for token in statement.tokens:
            if set_seen and isinstance(token, Comparison):
                set_items.append(str(token.left))
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'SET':
                set_seen = True
        return set_items

    def _extract_delete_target(self, statement) -> str:
        from_token = next(token for token in statement.tokens if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM')
        target_token = statement.token_next_by_instance(statement.token_index(from_token), Identifier)
        return target_token.get_real_name()

    def _extract_create_table_name(self, statement) -> str:
        create_token = next(token for token in statement.tokens if token.ttype is sqlparse.tokens.DDL and token.value.upper() == 'CREATE')
        table_token = statement.token_next_by_instance(statement.token_index(create_token), Identifier)
        return table_token.get_real_name()

    def _extract_create_columns(self, statement) -> List[str]:
        columns = []
        parenthesis = next(token for token in statement.tokens if isinstance(token, sqlparse.sql.Parenthesis))
        for token in parenthesis.tokens:
            if isinstance(token, Identifier):
                columns.append(token.get_real_name())
        return columns

# Usage
lineage_analyzer = SQLLineage()
query = """
WITH cte1 AS (
    SELECT id, name FROM users WHERE status = 'active'
), cte2 AS (
    SELECT o.id as order_id, o.user_id, o.total
    FROM orders o
    JOIN cte1 c ON o.user_id = c.id
    WHERE o.status = 'completed'
)
SELECT c.name, c2.order_id, c2.total,
       (SELECT AVG(total) FROM cte2 WHERE user_id = c.id) as avg_order
FROM cte1 c
LEFT JOIN cte2 c2 ON c.id = c2.user_id
WHERE c2.total > 100
"""
result = lineage_analyzer.parse_query(query)
print(result)
