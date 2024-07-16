import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Wildcard
from typing import Dict, Set, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLLineageTracer:
    def __init__(self):
        self.lineage: Dict[str, Set[str]] = {}
        self.ctes: Dict[str, str] = {}

    def trace_lineage(self, sql: str) -> Dict[str, Set[str]]:
        try:
            parsed = sqlparse.parse(sql)
            for statement in parsed:
                self._process_statement(statement)
            return self.lineage
        except Exception as e:
            logger.error(f"Error tracing lineage: {e}")
            return {}

    def _process_statement(self, statement):
        if statement.get_type() == 'SELECT':
            self._process_select(statement)
        elif statement.get_type() == 'INSERT':
            self._process_insert(statement)
        elif statement.get_type() == 'UPDATE':
            self._process_update(statement)
        elif statement.get_type() == 'DELETE':
            self._process_delete(statement)
        elif statement.get_type() == 'CREATE':
            self._process_create(statement)
        else:
            logger.warning(f"Unhandled statement type: {statement.get_type()}")

    def _process_select(self, statement):
        target_table = self._get_target_table(statement)
        from_tables = self._get_from_tables(statement)
        select_items = self._get_select_items(statement)

        for item in select_items:
            target_column = f"{target_table}.{item}"
            for table in from_tables:
                source_column = f"{table}.{item}"
                self._add_lineage(target_column, source_column)

        self._process_where_clause(statement, target_table)
        self._process_join_conditions(statement, target_table)

    def _process_insert(self, statement):
        target_table = self._get_target_table(statement)
        columns = self._get_insert_columns(statement)
        select_statement = self._get_insert_select(statement)

        if select_statement:
            self._process_select(select_statement)
            select_items = self._get_select_items(select_statement)
            for col, item in zip(columns, select_items):
                target_column = f"{target_table}.{col}"
                source_column = f"{self._get_target_table(select_statement)}.{item}"
                self._add_lineage(target_column, source_column)

    def _process_update(self, statement):
        target_table = self._get_target_table(statement)
        set_items = self._get_update_set_items(statement)

        for item in set_items:
            target_column = f"{target_table}.{item['column']}"
            source_columns = self._extract_columns_from_expression(item['value'])
            for source in source_columns:
                self._add_lineage(target_column, source)

        self._process_where_clause(statement, target_table)

    def _process_delete(self, statement):
        target_table = self._get_target_table(statement)
        self._process_where_clause(statement, target_table)

    def _process_create(self, statement):
        target_table = self._get_target_table(statement)
        select_statement = self._get_create_select(statement)
        if select_statement:
            self._process_select(select_statement)

    def _get_target_table(self, statement) -> str:
        if statement.get_type() in ('SELECT', 'DELETE'):
            from_clause = next(stmt for stmt in statement.tokens if isinstance(stmt, IdentifierList) or (isinstance(stmt, Identifier) and stmt.get_name() != 'AS'))
            return str(from_clause.get_real_name())
        elif statement.get_type() in ('INSERT', 'UPDATE', 'CREATE'):
            return str(next(token for token in statement.tokens if isinstance(token, Identifier)).get_real_name())
        else:
            return 'unknown_table'

    def _get_from_tables(self, statement) -> List[str]:
        from_seen = False
        tables = []
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.append(str(identifier.get_real_name()))
                elif isinstance(token, Identifier):
                    tables.append(str(token.get_real_name()))
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
        return tables

    def _get_select_items(self, statement) -> List[str]:
        select_seen = False
        items = []
        for token in statement.tokens:
            if select_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        items.append(str(identifier.get_real_name()))
                elif isinstance(token, Identifier):
                    items.append(str(token.get_real_name()))
                elif token.ttype is Wildcard:
                    items.append('*')
            if token.ttype is DML and token.value.upper() == 'SELECT':
                select_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('FROM', 'WHERE'):
                break
        return items

    def _get_insert_columns(self, statement) -> List[str]:
        columns_seen = False
        columns = []
        for token in statement.tokens:
            if columns_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        columns.append(str(identifier.get_real_name()))
                elif isinstance(token, Identifier):
                    columns.append(str(token.get_real_name()))
            if token.ttype is Keyword and token.value.upper() == 'INTO':
                columns_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('VALUES', 'SELECT'):
                break
        return columns

    def _get_insert_select(self, statement):
        for token in statement.tokens:
            if isinstance(token, IdentifierList) and token.tokens[0].ttype is DML and token.tokens[0].value.upper() == 'SELECT':
                return token
        return None

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
                                'value': str(identifier.right)
                            })
            if token.ttype is Keyword and token.value.upper() == 'SET':
                set_seen = True
            elif token.ttype is Keyword and token.value.upper() == 'WHERE':
                break
        return items

    def _get_create_select(self, statement):
        for token in statement.tokens:
            if isinstance(token, IdentifierList) and token.tokens[0].ttype is DML and token.tokens[0].value.upper() == 'SELECT':
                return token
        return None

    def _process_where_clause(self, statement, target_table):
        where_clause = next((token for token in statement.tokens if isinstance(token, Where)), None)
        if where_clause:
            columns = self._extract_columns_from_where(where_clause)
            for column in columns:
                self._add_lineage(f"{target_table}.{column}", column)

    def _process_join_conditions(self, statement, target_table):
        join_seen = False
        for token in statement.tokens:
            if join_seen and isinstance(token, Comparison):
                left = str(token.left)
                right = str(token.right)
                self._add_lineage(f"{target_table}.{left}", left)
                self._add_lineage(f"{target_table}.{right}", right)
            if token.ttype is Keyword and token.value.upper() in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'):
                join_seen = True
            elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP BY', 'HAVING', 'ORDER BY'):
                break

    def _extract_columns_from_where(self, where_clause) -> Set[str]:
        columns = set()
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                columns.add(str(token.left))
                columns.add(str(token.right))
            elif isinstance(token, Function):
                columns.update(self._extract_columns_from_function(token))
        return columns

    def _extract_columns_from_function(self, function) -> Set[str]:
        columns = set()
        for token in function.tokens:
            if isinstance(token, Identifier):
                columns.add(str(token))
            elif isinstance(token, Function):
                columns.update(self._extract_columns_from_function(token))
        return columns

    def _extract_columns_from_expression(self, expression) -> Set[str]:
        columns = set()
        tokens = sqlparse.parse(expression)[0].tokens
        for token in tokens:
            if isinstance(token, Identifier):
                columns.add(str(token))
            elif isinstance(token, Function):
                columns.update(self._extract_columns_from_function(token))
        return columns

    def _add_lineage(self, target: str, source: str):
        if target not in self.lineage:
            self.lineage[target] = set()
        self.lineage[target].add(source)

def print_lineage(lineage: Dict[str, Set[str]]):
    if lineage:
        print("Lineage relationships:")
        for target, sources in lineage.items():
            for source in sources:
                print(f"{source} -> {target}")
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
