import math

import narwhals as nw
import sqlglot
from sqlglot import expressions

engine = nw
# engine = pl


class SQLParser:
    """
    SQL Parser translating SQL string expression to a Narwhals Expression.
    """

    def __init__(self):
        self.context = {}

    def parse(self, sql: str) -> nw.Expr:
        """
        Parse SQL expression.

        Parameters
        ----------
        sql:
            SQL Expression

        Returns
        -------
        :
            Narwhals expression

        Examples
        --------
        ```py
        import laktory as lk

        parser = lk.SQLParser()

        print(parser.parse("2*x + y"))
        '''
        Narwhals Expr
        metadata: ExprMetadata(
          expansion_kind: ExpansionKind.SINGLE,
          last_node: ExprKind.NARY,
          has_windows: False,
          n_orderable_ops: 0,
          is_elementwise: False,
          preserves_length: True,
          is_scalar_like: False,
          is_literal: False,
        )
        '''
        ```
        """
        parsed_expr = sqlglot.parse_one(sql)
        # visitor = SQLExprVisitor(self.context)
        return self.visit_expr(parsed_expr)

    def visit_expr(self, expr):
        # print(f"Visiting expression {expr} of type {type(expr)}")

        # if isinstance(expr, expressions.All):
        #     return self.visit_all(expr.left, expr.compare_op, expr.right)
        #
        # elif isinstance(expr, expressions.Any):
        #     # 'is_some' is ignored.
        #     return self.visit_any(expr.left, expr.compare_op, expr.right)
        #
        # elif isinstance(expr, expressions.Array):
        #     # Assuming 'elem' is an attribute of the array.
        #     return self.visit_array_expr(expr.elem, True, None)
        #
        # elif isinstance(expr, expressions.Between):
        #     return self.visit_between(expr.expr, expr.negated, expr.low, expr.high)
        #

        if isinstance(expr, expressions.Column):
            if len(expr.parts) == 1:
                return engine.col(expr.this.name)
            elif len(expr.parts) == 2:
                return engine.col(expr.parts[0].this).struct.field(expr.parts[1].this)
            else:
                raise Exception(
                    f"Column expression {expr} with more than 2 parts is not supported"
                )

        if isinstance(expr, expressions.Binary):
            return self.visit_binary_op(expr)
        #
        if isinstance(expr, expressions.Cast):
            return self.visit_cast(expr)
        #
        # if isinstance(expr, expressions.Ceil):
        #     return self.visit_expr(expr.expr).ceil()
        #
        # if isinstance(expr, expressions.CompoundIdentifier):
        #     return self.visit_compound_identifier(expr.idents)
        #
        # if isinstance(expr, expressions.Extract):
        #     return parse_extract_date_part(self.visit_expr(expr.expr), expr.field)
        #
        # if isinstance(expr, expressions.Floor):
        #     return self.visit_expr(expr.expr).floor()
        #
        if isinstance(expr, expressions.Func):
            return self.visit_function(expr)
        #
        # if isinstance(expr, expressions.Identifier):
        #     return self.visit_identifier(expr.ident)
        #
        # if isinstance(expr, expressions.In):
        #     evaluated_expr = self.visit_expr(expr.expr)
        #     elems = self.visit_array_expr(expr.list, False, evaluated_expr)
        #     is_in = evaluated_expr.is_in(elems)
        #     return is_in.
        #     not () if expr.negated else is_in

        # if isinstance(expr, expressions.InSubquery):
        #     return self.visit_in_subquery(expr.expr, expr.subquery, expr.negated)
        #
        # if isinstance(expr, expressions.Interval):
        #     duration = interval_to_duration(expr.interval, True)
        #     return lit(duration)

        # if isinstance(expr, expressions.IsDistinctFrom):
        #     return self.visit_expr(expr.e1).neq_missing(self.visit_expr(expr.e2))

        # if isinstance(expr, expressions.IsFalse):
        #     return self.visit_expr(expr.expr).eq(lit(False))
        #
        # if isinstance(expr, expressions.IsNotDistinctFrom):
        #     return self.visit_expr(expr.e1).eq_missing(self.visit_expr(expr.e2))

        # if isinstance(expr, expressions.IsNotFalse):
        #     return self.visit_expr(expr.expr).eq(lit(False)).
        #     not ()

        # if isinstance(expr, expressions.IsNotNull):
        #     return self.visit_expr(expr.expr).is_not_null()
        #
        # if isinstance(expr, expressions.IsNotTrue):
        #     return self.visit_expr(expr.expr).eq(lit(True)).
        #     not ()
        #
        # if isinstance(expr, expressions.IsNull):
        #     return self.visit_expr(expr.expr).is_null()

        # if isinstance(expr, expressions.IsTrue):
        #     return self.visit_expr(expr.expr).eq(lit(True))
        #
        # if isinstance(expr, expressions.Like):
        #     if expr.any:
        #         raise Exception("LIKE ANY is not a supported syntax")
        #     return self.visit_like(expr.negated, expr.expr, expr.pattern, expr.escape_char, False)
        #
        # if isinstance(expr, expressions.ILike):
        #     if expr.any:
        #         raise Exception("ILIKE ANY is not a supported syntax")
        #     return self.visit_like(expr.negated, expr.expr, expr.pattern, expr.escape_char, True)
        #
        # if isinstance(expr, expressions.Nested):
        #     return self.visit_expr(expr.expr)
        #
        # if isinstance(expr, expressions.Position):
        #     # Note: SQL is 1-indexed.
        #     # Here we assume the attribute for the "in" part is renamed to avoid the Python keyword.
        #     found_expr = self.visit_expr(expr.in_).str().find(self.visit_expr(expr.expr), True)
        #     return (found_expr + typed_lit(1)).fill_null(typed_lit(0))
        #
        # if isinstance(expr, expressions.RLike):
        #     matches = self.visit_expr(expr.expr).str().contains(self.visit_expr(expr.pattern), True)
        #     return matches.
        #     not () if expr.negated else matches
        #
        # if isinstance(expr, expressions.Subscript):
        #     return self.visit_subscript(expr.expr, expr.subscript)
        #
        # if isinstance(expr, expressions.Subquery):
        #     raise Exception("unexpected subquery")
        #
        # if isinstance(expr, expressions.Trim):
        #     return self.visit_trim(expr.expr, expr.trim_where, expr.trim_what, expr.trim_characters)

        # if isinstance(expr, expressions.TypedString):
        #     dt = expr.data_type
        #     value = expr.value
        #     if dt == SQLDataType.Date:
        #         if is_iso_date(value):
        #             return lit(value).cast(DataType.Date)
        #         else:
        #             raise Exception(f"invalid DATE literal '{value}'")
        #     elif dt == SQLDataType.Time(None, TimezoneInfo.None):
        #         if is_iso_time(value):
        #             return lit(value).str().to_time(StrptimeOptions(strict=True))
        #         else:
        #             raise Exception(f"invalid TIME literal '{value}'")
        #     elif dt == SQLDataType.Timestamp(None, TimezoneInfo.None) or dt == SQLDataType.Datetime(None):
        #         if is_iso_datetime(value):
        #             return lit(value).str().to_datetime(
        #                 None,
        #                 None,
        #                 StrptimeOptions(strict=True),
        #                 lit("latest")
        #             )
        #         else:
        #             fn_name = "TIMESTAMP" if dt.__class__.__name__ == "Timestamp" else "DATETIME"
        #             raise Exception(f"invalid {fn_name} literal '{value}'")
        #     else:
        #         raise Exception(f"typed literal should be one of DATE, DATETIME, TIME, or TIMESTAMP (found {dt})")

        # if isinstance(expr, expressions.Unary):
        #     return self.visit_unary_op(expr.op, expr.expr)
        #
        if isinstance(expr, expressions.Literal):
            return self.visit_literal(expr)
        #
        # if isinstance(expr, expressions.Star):
        #     return Expr.Wildcard
        #
        if isinstance(expr, expressions.Case):
            return self.visit_case(expr)

        raise Exception(
            f"expression {expr} of type '{type(expr)}' is not currently supported"
        )

    def visit_literal(self, literal):
        if literal.is_int:
            return engine.lit(int(literal.name))

        if literal.is_number:
            return engine.lit(float(literal.name))

        return engine.lit(literal.this)

    def visit_binary_op(self, expr):
        expr_type = type(expr)
        left = self.visit_expr(expr.this)
        right = self.visit_expr(expr.expression)

        op_map = {
            expressions.Add: left.__add__,
            expressions.And: left.__and__,
            expressions.Div: left.__truediv__,
            expressions.EQ: left.__eq__,
            expressions.GT: left.__gt__,
            expressions.GTE: left.__ge__,
            expressions.LT: left.__lt__,
            expressions.LTE: left.__le__,
            expressions.Mod: left.__mod__,
            expressions.Mul: left.__mul__,
            expressions.NEQ: left.__ne__,
            expressions.Or: left.__or__,
            expressions.Sub: left.__sub__,
            expressions.Pow: left.__pow__,
        }

        if expr_type not in op_map:
            raise ValueError(f"Unsupported binary operator {expr_type}")

        return op_map[expr_type](right)

    def visit_function(self, expr):
        expr_type = type(expr)

        def _log(s):
            import numpy as np

            return np.log(s.to_numpy())

        def _log10(s):
            import numpy as np

            return np.log10(s.to_numpy())

        # Unary functions
        func_types = {
            # Math
            expressions.Abs: lambda x: x.abs,
            # TODO: Implement in Narwhals
            expressions.Cbrt: lambda x: x ** (1.0 / 3.0),
            # TODO: Implement in Narwhals
            expressions.Ceil: lambda x: (-1) * ((-1) * x) // 1,
            # TODO: Implement in Narwhals
            expressions.Exp: lambda x: math.e**x,
            # TODO: Implement in Narwhals
            expressions.Floor: lambda x: x // 1,
            # TODO: Implement in Narwhals
            expressions.Ln: lambda x: x.map_batches(_log),
            # TODO: Implement in Narwhals
            expressions.Log: lambda x: x.map_batches(_log10),
            # TODO: Log10? Log1p? Log2?
            expressions.Round: lambda x: x.round,
            expressions.Min: lambda x: x.min,
            expressions.Max: lambda x: x.max,
            expressions.Sum: lambda x: x.sum,
            expressions.Avg: lambda x: x.mean,
            expressions.Count: lambda x: x.len,
            # String
            expressions.Lower: lambda x: x.str.to_lowercase,
            expressions.Upper: lambda x: x.str.to_uppercase,
        }

        if expr_type in func_types:
            args = [expr.this] + expr.expressions

            decimals = expr.args.get("decimals", None)
            if decimals is not None:
                # TODO: Fix for round(x, 3)
                args += [decimals]

            args = [self.visit_expr(arg) for arg in args]

            # Convert literals to values
            for i in range(len(args)):
                if hasattr(args[i], "meta") and args[i].meta.is_literal():
                    args[i] = engine.select(args[i]).item()

            f = func_types[expr_type]
            f = f(args[0])

            if hasattr(f, "__call__"):
                f = f(*args[1:])
            return f

        expr_name = expr.this
        if isinstance(expr_name, str):
            expr_name = expr_name.lower()
        func_names = {"pi": engine.lit(math.pi)}

        # Nullary functions
        if expr_name in func_names:
            # args = [expr.this] + expr.expressions
            # args = [self.visit_expr(arg) for arg in args]
            f = func_names[expr_name]
            return f
            # f = f(args[0])
            # if hasattr(f, "__call__"):
            #     f = f(*args[1:])
            # return f

        raise ValueError(f"Unsupported function {expr_name} of type {expr_type}")

    def visit_function2(self, expr):
        pass

        # function = self.func
        #
        # # Check for unsupported clauses.
        # if function.within_group:
        #     raise Exception("'WITHIN GROUP' is not currently supported")
        # if function.filter is not None:
        #     raise Exception("'FILTER' is not currently supported")
        # if function.null_treatment is not None:
        #     raise Exception("'IGNORE|RESPECT NULLS' is not currently supported")
        #
        # # --- Bitwise functions ---
        # if function_name == "BitAnd":
        #     return self.visit_binary(lambda e, d: e.and_(d))
        # elif function_name == "BitCount":
        #     return self.visit_unary(lambda e: e.bitwise_count_ones())
        # elif function_name == "BitOr":
        #     return self.visit_binary(lambda e, d: e.or_(d))
        # elif function_name == "BitXor":
        #     return self.visit_binary(lambda e, d: e.xor(d))
        #
        # # --- Conditional functions ---
        # elif function_name == "If":
        #     args = extract_args(function)
        #     if len(args) == 3:
        #         # Assume try_visit_ternary takes a lambda with three arguments.
        #         return self.try_visit_ternary(
        #             lambda cond, expr1, expr2: when(cond).then(expr1).otherwise(expr2)
        #         )
        #     else:
        #         raise Exception(f"IF expects 3 arguments (found {len(args)})")
        # elif function_name == "IfNull":
        #     args = extract_args(function)
        #     if len(args) == 2:
        #         # For IFNULL we delegate to a variadic COALESCE implementation.
        #         return self.visit_variadic(coalesce)
        #     else:
        #         raise Exception(f"IFNULL expects 2 arguments (found {len(args)})")
        #
        # # --- Date functions (example: DATE_PART) ---
        # elif function_name == "DatePart":
        #     # Here, we expect two arguments: the part and the expression.
        #     return self.try_visit_binary(lambda part, e: (
        #         e.extract(DateTimeField.Custom(part.value))  # assume part.value holds the string value
        #         if isinstance(part, Expr.Literal) and isinstance(part.value, str)
        #         else (_ for _ in ()).throw(Exception(f"invalid 'part' for EXTRACT/DATE_PART: {part}"))
        #     ))
        #
        # # --- String functions (example: Lower) ---
        # elif function_name == "Lower":
        #     return self.visit_unary(lambda e: e.str().to_lowercase())
        #
        # # --- Column selection ---
        # elif function_name == "Columns":
        #     active_schema = self.active_schema
        #
        #     def process_columns(e):
        #         if isinstance(e, Expr.Literal) and isinstance(e.value, str):
        #             pat = e.value
        #             if pat == "*":
        #                 raise Exception("COLUMNS('*') is not a valid regex; did you mean COLUMNS(*)?")
        #             # Build a regex pattern similar to Rust version.
        #             if pat.startswith("^") and pat.endswith("$"):
        #                 regex_pat = pat
        #             elif pat.startswith("^"):
        #                 regex_pat = f"{pat}.*$"
        #             elif pat.endswith("$"):
        #                 regex_pat = f"^.*{pat}"
        #             else:
        #                 regex_pat = f"^.*{pat}.*$"
        #             if active_schema:
        #                 import re
        #                 rx = re.compile(regex_pat)
        #                 col_names = [name for name in active_schema.iter_names() if rx.match(name)]
        #                 if len(col_names) == 1:
        #                     return col(col_names[0])
        #                 else:
        #                     return cols(col_names)
        #             else:
        #                 return col(pat)
        #         elif isinstance(e, Expr.Wildcard):
        #             return col("*")
        #         else:
        #             raise Exception(f"COLUMNS expects a regex; found {e}")
        #
        #     return self.try_visit_unary(process_columns)
        #
        # # --- User-defined functions ---
        # elif function_name.startswith("Udf"):
        #     # Assume Udf function names are prefixed; extract and pass to visit_udf.
        #     udf_name = function_name[len("Udf"):]
        #     return self.visit_udf(udf_name)
        #
        # else:
        #     raise Exception(f"Unsupported function: {function_name}")

    # def try_visit_unary(self, f):
    #     """
    #     Try to visit a unary function by processing a single argument.
    #     If the argument is an expression, parse it and apply `f`.
    #     If it is a wildcard, create a wildcard SQL expression,
    #     parse it, and apply `f`. Finally, apply any window specification.
    #     """
    #     args = extract_args(self.func)
    #     # Expect exactly one argument
    #     if len(args) != 1:
    #         return self.not_supported_error()
    #
    #     arg = args[0]
    #     if arg.kind == "expr":
    #         # For an expression argument, parse and apply f.
    #         parsed_expr = parse_sql_expr(arg.value, self.ctx, self.active_schema)
    #         result = f(parsed_expr)
    #     elif arg.kind == "wildcard":
    #         # For a wildcard argument, create an equivalent SQLExpr wildcard.
    #         wildcard_expr = SQLExpr.Wildcard(AttachedToken.empty())
    #         parsed_expr = parse_sql_expr(wildcard_expr, self.ctx, self.active_schema)
    #         result = f(parsed_expr)
    #     else:
    #         return self.not_supported_error()
    #
    #     # Finally, if there is a window specification, apply it.
    #     return self.apply_window_spec(result, self.func.over)
    #
    # def visit_unary_with_opt_cumulative(self, f, cumulative_f):
    #     """
    #     Visit a unary function but, if there is a window specification,
    #     try to apply a cumulative variant.
    #
    #     If self.func.over is a WindowSpec, call apply_cumulative_window with f and cumulative_f.
    #     If it’s a NamedWindow, signal an error.
    #     Otherwise, fall back to the normal visit_unary behavior.
    #     """
    #     over_spec = self.func.over
    #     if over_spec is not None:
    #         if isinstance(over_spec, WindowSpec):
    #             return self.apply_cumulative_window(f, cumulative_f, over_spec)
    #         elif isinstance(over_spec, NamedWindow):
    #             raise Exception(f"Named windows are not currently supported; found {over_spec}")
    #     # Fall back to a standard unary visit
    #     return self.visit_unary(f)  # Assuming visit_unary is defined (perhaps similar to try_visit_unary)
    #
    # def visit_unary_no_window(self, f):
    #     """
    #     Visit a unary function ignoring any window specification.
    #     Only process a single expression argument.
    #     """
    #     args = extract_args(self.func)
    #     if len(args) != 1:
    #         return self.not_supported_error()
    #
    #     arg = args[0]
    #     if arg.kind == "expr":
    #         parsed_expr = parse_sql_expr(arg.value, self.ctx, self.active_schema)
    #         # Directly apply the function (e.g. turning SUM(a) into SUM)
    #         return f(parsed_expr)
    #     else:
    #         return self.not_supported_error()
    #
    #
    # def visit_case(self, expr):
    #     exprs = [self.visit_expr(e) for e in expr.args]
    #     when_then_pairs = zip(exprs[::2], exprs[1::2])
    #
    #     case_expr = None
    #     for when, then in when_then_pairs:
    #         case_expr = (
    #             when.then(then)
    #             if case_expr is None
    #             else case_expr.when(when).then(then)
    #         )
    #
    #     return case_expr.otherwise(exprs[-1]) if len(exprs) % 2 == 1 else case_expr

    def visit_cast(self, expr):
        dtype = self.map_sql_type(expr.to)
        return self.visit_expr(expr.this).cast(dtype)

    def map_sql_type(self, sql_type):
        types_map = {
            "INT": engine.Int64,
            "FLOAT": engine.Float64,
            "TEXT": engine.Utf8,
            "BOOLEAN": engine.Boolean,
            "DATE": engine.Date,
            "TIMESTAMP": engine.Datetime,
        }
        return types_map.get(sql_type.upper(), None)


#
# def extract_args_and_clauses(func):
#     """
#     Given a sqlglot function node (e.g. an instance of exp.Func),
#     extract:
#       - a list of its argument expressions (while erroring if any argument is a subquery),
#       - a Boolean indicating whether DISTINCT was applied,
#       - and any extra clauses (if present).
#     """
#     return _extract_func_args(func, get_distinct=True, get_clauses=True)
#
#
# def _extract_func_args(func, get_distinct, get_clauses):
#     # Check for DISTINCT. In sqlglot, a function with DISTINCT is usually marked
#     # via an attribute (for example in func.args["distinct"]).
#     is_distinct = bool(func.args.get("distinct", False))
#
#     # Assume any extra clauses are stored in func.args under the key "clauses"
#     clauses = func.args.get("clauses", [])
#
#     # If neither clauses nor distinct are expected, error if found.
#     if not (get_clauses or get_distinct) and is_distinct:
#         raise Exception(f"unexpected use of DISTINCT found in '{func.this}'")
#     elif not get_clauses and clauses:
#         raise Exception(f"unexpected clause found in '{func.this}' ({clauses[0]})")
#
#     unpacked_args = []
#     # In sqlglot the arguments are in the 'expressions' attribute.
#     for arg in func.expressions:
#         # If an argument is a subquery, we error (as in the Rust code).
#         from sqlglot import exp  # ensure exp is imported
#         if isinstance(arg, exp.Subquery):
#             raise Exception(f"subquery not expected in {func.this}")
#         # Otherwise, we simply collect the argument.
#         unpacked_args.append(arg)
#
#     return unpacked_args, is_distinct, clauses
#
