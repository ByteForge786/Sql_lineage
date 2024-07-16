 import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison, TokenList
from sqlparse.tokens import Keyword, DML, Wildcard, Name, Punctuation
from typing import Dict, Set, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLLineageTracer:
    def __init__(self):
        self.lineage: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
        self.ctes: Dict[str, TokenList] = {}

    def trace_lineage(self, sql: str) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
        try:
            statements = sqlparse.parse(sql)
            for statement in statements:
                self._process_statement(statement)
            return self.lineage
        except Exception as e:
            logger.error(f"Error tracing lineage: {e}")
            return {}

    def _process_statement(self, statement):
        if statement.get_type() == 'WITH':
            self._process_with(statement)
        elif statement.get_type() == 'SELECT':
            self._process_select(statement)
        elif statement.get_type() == 'INSERT':
            self._process_insert(statement)
        elif statement.get_type() == 'UPDATE':
            self._process_update(statement)
        elif statement.get_type() == 'DELETE':
            self._process_delete(statement)
        elif statement.get_type() == 'CREATE':
            self._process_create(statement)

    def _process_with(self, statement):
        for token in statement.tokens:
            if isinstance(token, Identifier):
                cte_name = token.get_real_name()
                cte_query = token.tokens[-1]
                self.ctes[cte_name] = cte_query
        main_query = statement.token_next_by(i=sqlparse.sql.Where)[1]
        self._process_statement(main_query)

    def _process_select(self, statement, target_table='derived_table'):
        from_tables = self._get_from_tables(statement)
        select_items = self._get_select_items(statement)

        for item in select_items:
            target_column = item['alias'] if item['alias'] else item['name']
            source_columns = self._extract_source_columns(item['expr'], from_tables)
            for source in source_columns:
                self._add_lineage((target_table, target_column), source)

        self._process_joins(statement, target_table)
        self._process_where(statement, target_table)
        self._process_group_by(statement, target_table)
        self._process_having(statement, target_table)

    def _process_insert(self, statement):
        target_table = self._get_insert_target_table(statement)
        target_columns = self._get_insert_columns(statement)
        select_statement = self._get_insert_select(statement)

        if select_statement:
            select_items = self._get_select_items(select_statement)
            for target_col, select_item in zip(target_columns, select_items):
                source_columns = self._extract_source_columns(select_item['expr'], self._get_from_tables(select_statement))
                for source in source_columns:
                    self._add_lineage((target_table, target_col), source)

    def _process_update(self, statement):
        target_table = self._get_update_target_table(statement)
        set_items = self._get_update_set_items(statement)

        for item in set_items:
            target_column = item['column']
            source_columns = self._extract_source_columns(item['value'], [target_table])
            for source in source_columns:
                self._add_lineage((target_table, target_column), source)

        self._process_where(statement, target_table)

    def _process_delete(self, statement):
        target_table = self._get_delete_target_table(statement)
        self._process_where(statement, target_table)

    def _process_create(self, statement):
        target_table = self._get_create_target_table(statement)
        select_statement = self._get_create_select(statement)
        if select_statement:
            self._process_select(select_statement, target_table)

    def _get_from_tables(self, statement) -> List[str]:
        from_seen = False
        tables = []
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.append(self._get_table_name(identifier))
                elif isinstance(token, Identifier):
                    tables.append(self._get_table_name(token))
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
            if token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP', 'HAVING', 'ORDER'):
                break
        return tables

    def _get_table_name(self, identifier):
        if identifier.has_alias():
            return identifier.get_alias()
        elif identifier.get_real_name() in self.ctes:
            return identifier.get_real_name()
        else:
            return str(identifier.get_real_name())

    def _get_select_items(self, statement) -> List[Dict]:
        select_seen = False
        items = []
        for token in statement.tokens:
            if select_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        items.append(self._parse_select_item(identifier))
                elif isinstance(token, Identifier):
                    items.append(self._parse_select_item(token))
                elif token.ttype is Wildcard:
                    items.append({'name': '*', 'alias': None, 'expr': token})
            if token.ttype is DML and token.value.upper() == 'SELECT':
                select_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('FROM', 'WHERE'):
                break
        return items

    def _parse_select_item(self, item):
        if item.has_alias():
            return {'name': str(item.get_real_name()), 'alias': str(item.get_alias()), 'expr': item}
        else:
            return {'name': str(item.get_real_name()), 'alias': None, 'expr': item}

    def _get_insert_target_table(self, statement) -> str:
        for token in statement.tokens:
            if isinstance(token, Function) and token.get_name().upper() == 'INTO':
                return str(token.tokens[-1])
        return 'unknown_table'

    def _get_insert_columns(self, statement) -> List[str]:
        for token in statement.tokens:
            if isinstance(token, Function) and token.get_name().upper() == 'INTO':
                if isinstance(token.tokens[-1], Identifier):
                    return [str(col) for col in token.tokens[-1].tokens if isinstance(col, Identifier)]
        return []

    def _get_insert_select(self, statement):
        for token in statement.tokens:
            if isinstance(token, IdentifierList) and token.tokens[0].ttype is DML and token.tokens[0].value.upper() == 'SELECT':
                return token
        return None

    def _get_update_target_table(self, statement) -> str:
        for token in statement.tokens:
            if token.ttype is Keyword and token.value.upper() == 'UPDATE':
                return str(statement.tokens[statement.tokens.index(token) + 1])
        return 'unknown_table'

    def _get_update_set_items(self, statement) -> List[Dict[str, str]]:
        set_seen = False
        items = []
        for token in statement.tokens:
            if set_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, Comparison):
                            items.append({
                                'column': str(identifier.left),
                                'value': identifier.right
                            })
            if token.ttype is Keyword and token.value.upper() == 'SET':
                set_seen = True
            elif token.ttype is Keyword and token.value.upper() == 'WHERE':
                break
        return items

    def _get_delete_target_table(self, statement) -> str:
        from_seen = False
        for token in statement.tokens:
            if from_seen and isinstance(token, Identifier):
                return str(token)
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
        return 'unknown_table'

    def _get_create_target_table(self, statement) -> str:
        for token in statement.tokens:
            if isinstance(token, Identifier):
                return str(token)
        return 'unknown_table'

    def _get_create_select(self, statement):
        for token in statement.tokens:
            if isinstance(token, IdentifierList) and token.tokens[0].ttype is DML and token.tokens[0].value.upper() == 'SELECT':
                return token
        return None

    def _extract_source_columns(self, expr, from_tables) -> List[Tuple[str, str]]:
        if isinstance(expr, Identifier):
            if expr.has_alias():
                return self._extract_source_columns(expr.tokens[0], from_tables)
            else:
                for table in from_tables:
                    if expr.get_parent_name() == table or expr.get_parent_name() is None:
                        return [(table, str(expr.get_real_name()))]
        elif isinstance(expr, Function):
            sources = []
            for arg in expr.get_parameters():
                sources.extend(self._extract_source_columns(arg, from_tables))
            return sources
        elif isinstance(expr, Comparison):
            return self._extract_source_columns(expr.left, from_tables) + self._extract_source_columns(expr.right, from_tables)
        elif isinstance(expr, TokenList):
            sources = []
            for token in expr.tokens:
                if not token.is_whitespace and token.ttype is not Punctuation:
                    sources.extend(self._extract_source_columns(token, from_tables))
            return sources
        return []

    def _process_joins(self, statement, target_table):
        join_seen = False
        for token in statement.tokens:
            if join_seen and isinstance(token, Comparison):
                left_sources = self._extract_source_columns(token.left, [target_table])
                right_sources = self._extract_source_columns(token.right, [target_table])
                for left in left_sources:
                    for right in right_sources:
                        self._add_lineage((target_table, left[1]), right)
                        self._add_lineage((target_table, right[1]), left)
            if token.ttype is Keyword and token.value.upper() in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'):
                join_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP BY', 'HAVING', 'ORDER BY'):
                break

    def _process_where(self, statement, target_table):
        where_clause = statement.token_next_by(i=sqlparse.sql.Where)
        if where_clause:
            self._process_condition(where_clause[1], target_table)

    def _process_group_by(self, statement, target_table):
        group_by_seen = False
        for token in statement.tokens:
            if group_by_seen and isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    sources = self._extract_source_columns(identifier, [target_table])
                    for source in sources:
                        self._add_lineage((target_table, str(identifier)), source)
            if token.ttype is Keyword and token.value.upper() == 'GROUP BY':
                group_by_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('HAVING', 'ORDER BY'):
                break

    def _process_having(self, statement, target_table):
        having_clause = statement.token_next_by(m=(Keyword, 'HAVING'))
        if having_clause:
            self._process_condition(having_clause[1], target_table)

    def _process_condition(self, condition, target_table):
        if isinstance(condition, Comparison):
            left_sources = self._extract_source_columns(condition.left, [target_table])
            right_sources = self._extract_source_columns(condition.right, [target_table])
            for left in left_sources:
                for right in right_sources:
                    self._add_lineage((target_table, left[1]), right)
                    self._add_lineage((target_table, right[1]), left)
        elif isinstance(condition, TokenList):
            for token in condition.tokens:
                if isinstance(token, Comparison):
                    self._process_condition(token, target_table)

    def _add_lineage(self, target: Tuple[str, str], source: Tuple[str, str]):
        if target != source:  # Avoid self-referential lineage
            if target not in self.lineage:
                self.lineage[target] = set()
            self.lineage[target].add(source)

def print_lineage(lineage: Dict[Tuple[str, str], Set[Tuple[str, str]]]):
    if lineage:
        print("Lineage relationships:")
        for target, sources in lineage.items():
            for source in sources:
                print(f"{source[0]}.{source[1]} -> {target[0]}.{target[1]}")
    else:
        print("No lineage relationships found.")

# Usage
if __name__ == "__main__":
    tracer = SQLLineageTracer()
    sql = """
    WITH employee_cte AS (
        SELECT employee_id, first_name, last_name, department_id, salary
        FROM employees
        WHERE department_id = 5
    ),
    department_cte AS (
        SELECT department_id, department_name
        FROM departments
        WHERE location_id = 1700
    )
    INSERT INTO high_salary_employees (employee_id, full_name, department, salary)
    SELECT 
        e.employee_id,
        CONCAT(e.first_name, ' ', e.last_name) as full_name,
        d.department_name,
        e.salary
    FROM 
        employee_cte e
    JOIN 
        department_cte d ON e.department_id = d.department_id
    WHERE 
        e.salary > (SELECT AVG(salary) FROM employees)
    UNION ALL
    SELECT 
        e.employee_id,
        CONCAT(e.first_name, ' ', e.last_name) as full_name,
        'Executive' as department,
        e.salary
    FROM 
        employees e
    WHERE 
        e.employee_id IN (SELECT manager_id FROM departments)
    """

    lineage = tracer.trace_lineage(sql)
    print_lineage(lineage)
