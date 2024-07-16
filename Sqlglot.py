import sqlglot
from sqlglot import exp
from typing import Dict, List, Set, Tuple, Optional
import networkx as nx
import matplotlib.pyplot as plt

class SQLLineageTracer:
    def __init__(self):
        self.lineage: Dict[str, Set[str]] = {}
        self.ctes: Dict[str, exp.Expression] = {}
        self.graph = nx.DiGraph()

    def trace_lineage(self, sql: str) -> Dict[str, Set[str]]:
        try:
            parsed = sqlglot.parse_one(sql)
            self._process_node(parsed)
            self._visualize_lineage()
            return self.lineage
        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return {}

    def _process_node(self, node: exp.Expression, target_table: Optional[str] = None):
        if isinstance(node, exp.Select):
            self._handle_select(node, target_table)
        elif isinstance(node, exp.Union):
            self._handle_union(node, target_table)
        elif isinstance(node, exp.Insert):
            self._handle_insert(node)
        elif isinstance(node, exp.Update):
            self._handle_update(node)
        elif isinstance(node, exp.Delete):
            self._handle_delete(node)
        elif isinstance(node, exp.Create):
            self._handle_create(node)
        elif isinstance(node, exp.With):
            self._handle_with(node)

    def _handle_select(self, node: exp.Select, target_table: Optional[str]):
        if not target_table:
            target_table = node.alias or 'derived_table'

        for expr in node.expressions:
            target_column = f"{target_table}.{expr.alias_or_name}"
            source_columns = self._get_source_columns(expr)
            for source in source_columns:
                self._add_lineage(target_column, source)

        if node.from_:
            self._process_from(node.from_, target_table)

        if node.where:
            self._process_where(node.where, target_table)

        if node.group:
            self._process_group_by(node.group, target_table)

        if node.having:
            self._process_having(node.having, target_table)

    def _handle_union(self, node: exp.Union, target_table: Optional[str]):
        for select in node.expressions:
            self._handle_select(select, target_table)

    def _handle_insert(self, node: exp.Insert):
        # Check if 'into' is an attribute or a method
        if hasattr(node, 'into'):
            if callable(node.into):
                target_table = node.into().name
            else:
                target_table = node.into.name
        else:
            # If 'into' is not available, try to get the table name from the 'expression' attribute
            target_table = node.expression.name if hasattr(node, 'expression') else 'unknown_table'

        if isinstance(node.expression, exp.Select):
            self._handle_select(node.expression, target_table)

    def _handle_update(self, node: exp.Update):
        target_table = node.expression.name
        for set_item in node.set:
            target_column = f"{target_table}.{set_item.key}"
            source_columns = self._get_source_columns(set_item.value)
            for source in source_columns:
                self._add_lineage(target_column, source)

        if node.where:
            self._process_where(node.where, target_table)

    def _handle_delete(self, node: exp.Delete):
        target_table = node.expression.name
        if node.where:
            self._process_where(node.where, target_table)

    def _handle_create(self, node: exp.Create):
        if isinstance(node.expression, exp.Select):
            target_table = node.this.name
            self._handle_select(node.expression, target_table)

    def _handle_with(self, node: exp.With):
        for cte in node.expressions:
            self.ctes[cte.alias] = cte.expression
            self._process_node(cte.expression, cte.alias)
        self._process_node(node.expression)

    def _get_source_columns(self, expr: exp.Expression) -> List[str]:
        if isinstance(expr, exp.Column):
            return [f"{expr.table}.{expr.name}" if expr.table else expr.name]
        elif isinstance(expr, exp.Function):
            return [col for arg in expr.args for col in self._get_source_columns(arg)]
        elif isinstance(expr, exp.Binary):
            return self._get_source_columns(expr.left) + self._get_source_columns(expr.right)
        elif isinstance(expr, exp.Subquery):
            subquery_table = 'subquery'
            self._handle_select(expr.this, subquery_table)
            return [f"{subquery_table}.{col.alias_or_name}" for col in expr.this.expressions]
        return []

    def _process_from(self, from_expr: exp.Expression, target_table: str):
        if isinstance(from_expr, exp.Join):
            self._process_join(from_expr, target_table)
        elif isinstance(from_expr, exp.Subquery):
            self._handle_select(from_expr.this, from_expr.alias)
        elif isinstance(from_expr, exp.Table):
            pass  # Base table, no further processing needed
        else:
            self._process_node(from_expr, target_table)

    def _process_join(self, join: exp.Join, target_table: str):
        self._process_from(join.left, target_table)
        self._process_from(join.right, target_table)
        if join.on:
            self._process_where(join.on, target_table)

    def _process_where(self, where: exp.Expression, target_table: str):
        if isinstance(where, exp.Binary):
            self._process_where(where.left, target_table)
            self._process_where(where.right, target_table)
        elif isinstance(where, exp.Column):
            target_column = f"{target_table}.{where.name}"
            source_column = f"{where.table}.{where.name}" if where.table else where.name
            self._add_lineage(target_column, source_column)
        elif isinstance(where, exp.Subquery):
            subquery_table = 'subquery'
            self._handle_select(where.this, subquery_table)

    def _process_group_by(self, group_by: List[exp.Expression], target_table: str):
        for expr in group_by:
            source_columns = self._get_source_columns(expr)
            for source in source_columns:
                self._add_lineage(f"{target_table}.{expr.name}", source)

    def _process_having(self, having: exp.Expression, target_table: str):
        self._process_where(having, target_table)

    def _add_lineage(self, target: str, source: str):
        if target not in self.lineage:
            self.lineage[target] = set()
        self.lineage[target].add(source)
        self.graph.add_edge(source, target)

    def _visualize_lineage(self):
        plt.figure(figsize=(20, 10))
        pos = nx.spring_layout(self.graph)
        nx.draw(self.graph, pos, with_labels=True, node_color='lightblue', 
                node_size=3000, font_size=8, arrows=True)
        nx.draw_networkx_labels(self.graph, pos)
        plt.title("Data Lineage Graph")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig("lineage_graph.png", format="png", dpi=300, bbox_inches='tight')
        plt.close()

# Usage remains the same
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
for target, sources in lineage.items():
    for source in sources:
        print(f"{source} -> {target}")

print("\nLineage graph has been saved as 'lineage_graph.png'")
