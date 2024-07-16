import sqlglot
from sqlglot import exp
from typing import Dict, List, Set, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLLineageTracer:
    def __init__(self):
        self.lineage: Dict[str, Set[str]] = {}
        self.ctes: Dict[str, exp.Expression] = {}
        self.table_aliases: Dict[str, str] = {}

    def trace_lineage(self, sql: str) -> Dict[str, Set[str]]:
        try:
            parsed = sqlglot.parse_one(sql)
            self._process_node(parsed)
            return self.lineage
        except Exception as e:
            logger.error(f"Error tracing lineage: {e}")
            return {}

    def _process_node(self, node: exp.Expression, context: Optional[str] = None):
        if isinstance(node, exp.Select):
            self._process_select(node, context)
        elif isinstance(node, exp.Union):
            self._process_union(node, context)
        elif isinstance(node, exp.Insert):
            self._process_insert(node)
        elif isinstance(node, exp.Update):
            self._process_update(node)
        elif isinstance(node, exp.Delete):
            self._process_delete(node)
        elif isinstance(node, exp.With):
            self._process_with(node)
        elif isinstance(node, exp.Create):
            self._process_create(node)

    def _process_select(self, node: exp.Select, context: Optional[str]):
        target_table = context or node.alias or self._get_from_table(node.from_)
        
        for expr in node.expressions:
            target_column = f"{target_table}.{expr.alias or expr.name}"
            sources = self._get_column_sources(expr)
            for source in sources:
                self._add_lineage(target_column, source)

        if node.from_:
            self._process_from(node.from_, target_table)

        if node.where:
            self._process_condition(node.where, target_table)

        if node.group:
            for expr in node.group:
                self._process_expression(expr, target_table)

        if node.having:
            self._process_condition(node.having, target_table)

        if node.order:
            for expr in node.order:
                self._process_expression(expr.expression, target_table)

    def _process_union(self, node: exp.Union, context: Optional[str]):
        for select in node.expressions:
            self._process_select(select, context)

    def _process_insert(self, node: exp.Insert):
        target_table = node.into.name
        if isinstance(node.expression, exp.Select):
            self._process_select(node.expression, target_table)
        elif isinstance(node.expression, exp.Values):
            for column, value in zip(node.columns, node.expression.expressions):
                target_column = f"{target_table}.{column.name}"
                self._add_lineage(target_column, 'literal_value')

    def _process_update(self, node: exp.Update):
        target_table = node.expression.name
        for set_item in node.set:
            target_column = f"{target_table}.{set_item.key}"
            sources = self._get_column_sources(set_item.value)
            for source in sources:
                self._add_lineage(target_column, source)
        
        if node.where:
            self._process_condition(node.where, target_table)

    def _process_delete(self, node: exp.Delete):
        target_table = node.expression.name
        if node.where:
            self._process_condition(node.where, target_table)

    def _process_with(self, node: exp.With):
        for cte in node.expressions:
            self.ctes[cte.alias] = cte.expression
            self._process_node(cte.expression, cte.alias)
        self._process_node(node.expression)

    def _process_create(self, node: exp.Create):
        if isinstance(node.expression, exp.Select):
            self._process_select(node.expression, node.this.name)

    def _process_from(self, from_item: exp.Expression, context: str):
        if isinstance(from_item, exp.Table):
            self.table_aliases[from_item.alias or from_item.name] = from_item.name
        elif isinstance(from_item, exp.Subquery):
            self._process_select(from_item.this, from_item.alias)
        elif isinstance(from_item, exp.Join):
            self._process_from(from_item.left, context)
            self._process_from(from_item.right, context)
            if from_item.on:
                self._process_condition(from_item.on, context)

    def _process_condition(self, condition: exp.Expression, context: str):
        if isinstance(condition, exp.Binary):
            self._process_expression(condition.left, context)
            self._process_expression(condition.right, context)
        elif isinstance(condition, exp.Paren):
            self._process_condition(condition.this, context)
        else:
            self._process_expression(condition, context)

    def _process_expression(self, expr: exp.Expression, context: str):
        sources = self._get_column_sources(expr)
        for source in sources:
            self._add_lineage(f"{context}.{expr.alias or expr.name}", source)

    def _get_column_sources(self, expr: exp.Expression) -> List[str]:
        if isinstance(expr, exp.Column):
            return [f"{expr.table}.{expr.name}" if expr.table else expr.name]
        elif isinstance(expr, exp.Function):
            return [source for arg in expr.args for source in self._get_column_sources(arg)]
        elif isinstance(expr, exp.Binary):
            return self._get_column_sources(expr.left) + self._get_column_sources(expr.right)
        elif isinstance(expr, exp.Paren):
            return self._get_column_sources(expr.this)
        elif isinstance(expr, exp.Subquery):
            subquery_alias = expr.alias or 'subquery'
            self._process_select(expr.this, subquery_alias)
            return [f"{subquery_alias}.{col.alias or col.name}" for col in expr.this.expressions]
        elif isinstance(expr, exp.Case):
            sources = []
            for condition in expr.ifs:
                sources.extend(self._get_column_sources(condition.this))
                sources.extend(self._get_column_sources(condition.true))
            if expr.default:
                sources.extend(self._get_column_sources(expr.default))
            return sources
        return []

    def _get_from_table(self, from_item: exp.Expression) -> str:
        if isinstance(from_item, exp.Table):
            return from_item.alias or from_item.name
        elif isinstance(from_item, exp.Subquery):
            return from_item.alias or 'derived_table'
        elif isinstance(from_item, exp.Join):
            return self._get_from_table(from_item.left)
        return 'unknown_table'

    def _add_lineage(self, target: str, source: str):
        target_parts = target.split('.')
        source_parts = source.split('.')
        
        if len(target_parts) == 2:
            target = f"{self._get_full_table_name(target_parts[0])}.{target_parts[1]}"
        
        if len(source_parts) == 2:
            source = f"{self._get_full_table_name(source_parts[0])}.{source_parts[1]}"

        if target not in self.lineage:
            self.lineage[target] = set()
        self.lineage[target].add(source)

    def _get_full_table_name(self, alias: str) -> str:
        return self.table_aliases.get(alias, alias)

def print_lineage(lineage: Dict[str, Set[str]]):
    if lineage:
        print("Column Lineage:")
        for target, sources in lineage.items():
            for source in sources:
                print(f"{source} -> {target}")
    else:
        print("No lineage relationships found.")

# Usage example
if __name__ == "__main__":
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

    tracer = SQLLineageTracer()
    lineage = tracer.trace_lineage(sql)
    print_lineage(lineage)
