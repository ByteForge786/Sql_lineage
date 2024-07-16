import sqlglot
from typing import Dict, List, Set, Tuple, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLLineageTracer:
    def __init__(self):
        self.lineage: Dict[str, Set[str]] = {}
        self.ctes: Dict[str, sqlglot.expressions.Expression] = {}
        self.table_aliases: Dict[str, str] = {}

    def trace_lineage(self, sql: str) -> Dict[str, Set[str]]:
        try:
            parsed = sqlglot.parse_one(sql)
            logger.info("SQL parsed successfully")
            self._process_node(parsed)
            return self.lineage
        except sqlglot.ParseError as e:
            logger.error(f"SQL parsing error: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error during lineage tracing: {e}")
            return {}

    def _process_node(self, node: sqlglot.expressions.Expression, target_table: Optional[str] = None):
        try:
            if isinstance(node, sqlglot.expressions.Select):
                self._handle_select(node, target_table)
            elif isinstance(node, sqlglot.expressions.Union):
                self._handle_union(node, target_table)
            elif isinstance(node, sqlglot.expressions.Insert):
                self._handle_insert(node)
            elif isinstance(node, sqlglot.expressions.Update):
                self._handle_update(node)
            elif isinstance(node, sqlglot.expressions.Delete):
                self._handle_delete(node)
            elif isinstance(node, sqlglot.expressions.Create):
                self._handle_create(node)
            elif isinstance(node, sqlglot.expressions.With):
                self._handle_with(node)
            elif isinstance(node, sqlglot.expressions.Merge):
                self._handle_merge(node)
            else:
                logger.warning(f"Unhandled node type: {type(node)}")
        except Exception as e:
            logger.error(f"Error processing node {type(node)}: {e}")

    def _handle_select(self, node: sqlglot.expressions.Select, target_table: Optional[str]):
        if not target_table:
            target_table = node.alias or self._get_from_table(node.from_)
        logger.debug(f"Processing SELECT for target table: {target_table}")

        for expr in node.selects:
            target_column = f"{target_table}.{expr.alias_or_name}"
            source_columns = self._get_source_columns(expr)
            for source in source_columns:
                self._add_lineage(target_column, source)

        if node.from_:
            self._process_from(node.from_, target_table)

        if node.where:
            self._process_where(node.where, target_table)

        if node.group_by:
            self._process_group_by(node.group_by, target_table)

        if node.having:
            self._process_having(node.having, target_table)

        if hasattr(node, 'order_by') and node.order_by:
            self._process_order_by(node.order_by, target_table)

    def _get_from_table(self, from_clause):
        if isinstance(from_clause, sqlglot.expressions.Table):
            return from_clause.name
        elif isinstance(from_clause, sqlglot.expressions.Subquery):
            return from_clause.alias or 'derived_table'
        elif isinstance(from_clause, sqlglot.expressions.Join):
            return self._get_from_table(from_clause.left)
        return 'derived_table'

    def _handle_union(self, node: sqlglot.expressions.Union, target_table: Optional[str]):
        logger.debug("Processing UNION")
        for select in node.expressions:
            self._handle_select(select, target_table)

    def _handle_insert(self, node: sqlglot.expressions.Insert):
        logger.debug("Processing INSERT")
        target_table = node.into.name if node.into else 'unknown_table'
        logger.debug(f"Insert target table: {target_table}")

        if isinstance(node.expression, sqlglot.expressions.Select):
            self._handle_select(node.expression, target_table)
        elif isinstance(node.expression, sqlglot.expressions.Values):
            for column, value in zip(node.columns, node.expression.expressions):
                target_column = f"{target_table}.{column.name}"
                self._add_lineage(target_column, 'literal_value')

    def _handle_update(self, node: sqlglot.expressions.Update):
        logger.debug("Processing UPDATE")
        target_table = node.expression.name
        for set_item in node.set:
            target_column = f"{target_table}.{set_item.key}"
            source_columns = self._get_source_columns(set_item.value)
            for source in source_columns:
                self._add_lineage(target_column, source)

        if node.where:
            self._process_where(node.where, target_table)

    def _handle_delete(self, node: sqlglot.expressions.Delete):
        logger.debug("Processing DELETE")
        target_table = node.expression.name
        if node.where:
            self._process_where(node.where, target_table)

    def _handle_create(self, node: sqlglot.expressions.Create):
        logger.debug("Processing CREATE")
        if isinstance(node.expression, sqlglot.expressions.Select):
            target_table = node.this.name
            self._handle_select(node.expression, target_table)

    def _handle_with(self, node: sqlglot.expressions.With):
        logger.debug("Processing WITH")
        for cte in node.expressions:
            self.ctes[cte.alias] = cte.expression
            self._process_node(cte.expression, cte.alias)
        self._process_node(node.expression)

    def _handle_merge(self, node: sqlglot.expressions.Merge):
        logger.debug("Processing MERGE")
        target_table = node.into.name
        self._process_from(node.into, target_table)
        if node.on:
            self._process_where(node.on, target_table)
        for clause in node.clauses:
            if isinstance(clause, sqlglot.expressions.When):
                if clause.matched:
                    if clause.update:
                        for set_item in clause.update:
                            target_column = f"{target_table}.{set_item.key}"
                            source_columns = self._get_source_columns(set_item.value)
                            for source in source_columns:
                                self._add_lineage(target_column, source)
                else:
                    if clause.insert:
                        for column, value in zip(clause.insert.columns, clause.insert.values):
                            target_column = f"{target_table}.{column.name}"
                            source_columns = self._get_source_columns(value)
                            for source in source_columns:
                                self._add_lineage(target_column, source)

    def _get_source_columns(self, expr: sqlglot.expressions.Expression) -> List[str]:
        try:
            if isinstance(expr, sqlglot.expressions.Column):
                return [f"{expr.table}.{expr.name}" if expr.table else expr.name]
            elif isinstance(expr, sqlglot.expressions.Function):
                return [col for arg in expr.args for col in self._get_source_columns(arg)]
            elif isinstance(expr, sqlglot.expressions.Binary):
                return self._get_source_columns(expr.left) + self._get_source_columns(expr.right)
            elif isinstance(expr, sqlglot.expressions.Subquery):
                subquery_table = 'subquery'
                self._handle_select(expr.this, subquery_table)
                return [f"{subquery_table}.{col.alias_or_name}" for col in expr.this.selects]
            elif isinstance(expr, sqlglot.expressions.Literal):
                return ['literal_value']
            elif isinstance(expr, sqlglot.expressions.Case):
                sources = []
                for condition in expr.ifs:
                    sources.extend(self._get_source_columns(condition.this))
                    sources.extend(self._get_source_columns(condition.expression))
                if expr.else_:
                    sources.extend(self._get_source_columns(expr.else_))
                return sources
            else:
                logger.warning(f"Unhandled expression type in _get_source_columns: {type(expr)}")
                return []
        except Exception as e:
            logger.error(f"Error in _get_source_columns: {e}")
            return []

    def _process_from(self, from_expr: sqlglot.expressions.Expression, target_table: str):
        try:
            if isinstance(from_expr, sqlglot.expressions.Join):
                self._process_join(from_expr, target_table)
            elif isinstance(from_expr, sqlglot.expressions.Subquery):
                self._handle_select(from_expr.this, from_expr.alias)
            elif isinstance(from_expr, sqlglot.expressions.Table):
                self.table_aliases[from_expr.alias or from_expr.name] = from_expr.name
            else:
                self._process_node(from_expr, target_table)
        except Exception as e:
            logger.error(f"Error in _process_from: {e}")

    def _process_join(self, join: sqlglot.expressions.Join, target_table: str):
        try:
            self._process_from(join.left, target_table)
            self._process_from(join.right, target_table)
            if join.on:
                self._process_where(join.on, target_table)
        except Exception as e:
            logger.error(f"Error in _process_join: {e}")

    def _process_where(self, where: sqlglot.expressions.Expression, target_table: str):
        try:
            if isinstance(where, sqlglot.expressions.Binary):
                self._process_where(where.left, target_table)
                self._process_where(where.right, target_table)
            elif isinstance(where, sqlglot.expressions.Column):
                target_column = f"{target_table}.{where.name}"
                source_column = f"{where.table}.{where.name}" if where.table else where.name
                self._add_lineage(target_column, source_column)
            elif isinstance(where, sqlglot.expressions.Subquery):
                subquery_table = 'subquery'
                self._handle_select(where.this, subquery_table)
        except Exception as e:
            logger.error(f"Error in _process_where: {e}")

    def _process_group_by(self, group_by: List[sqlglot.expressions.Expression], target_table: str):
        try:
            for expr in group_by:
                source_columns = self._get_source_columns(expr)
                for source in source_columns:
                    self._add_lineage(f"{target_table}.{expr.name}", source)
        except Exception as e:
            logger.error(f"Error in _process_group_by: {e}")

    def _process_having(self, having: sqlglot.expressions.Expression, target_table: str):
        try:
            self._process_where(having, target_table)
        except Exception as e:
            logger.error(f"Error in _process_having: {e}")

    def _process_order_by(self, order_by: List[sqlglot.expressions.Expression], target_table: str):
        try:
            for expr in order_by:
                source_columns = self._get_source_columns(expr.expression)
                for source in source_columns:
                    self._add_lineage(f"{target_table}.{expr.expression.name}", source)
        except Exception as e:
            logger.error(f"Error in _process_order_by: {e}")

    def _add_lineage(self, target: str, source: str):
        logger.debug(f"Adding lineage: {source} -> {target}")
        
        # Replace alias with full table name for target
        target_parts = target.split('.')
        if len(target_parts) == 2:
            alias, column = target_parts
            full_table_name = self._get_full_table_name(alias)
            target = f"{full_table_name}.{column}"

        # Replace alias with full table name for source
        source_parts = source.split('.')
        if len(source_parts) == 2:
            alias, column = source_parts
            full_table_name = self._get_full_table_name(alias)
            source = f"{full_table_name}.{column}"

        if target not in self.lineage:
            self.lineage[target] = set()
        self.lineage[target].add(source)

    def _get_full_table_name(self, alias: str) -> str:
        return self.table_aliases.get(alias, alias)

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
