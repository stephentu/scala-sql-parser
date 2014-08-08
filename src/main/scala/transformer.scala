package com.stephentu.sql

trait Transformers {

  def topDownTransformation(n: Node)(f: Node => (Option[Node], Boolean)): Node =
    topDownTransformationWithParent(n)( (p: Option[Node], c: Node) => f(c) )

  // order:
  // 1. visit node (call f() on node)
  // 2. recurse on children (if node changes, recurse on *new* children)
  // 3. if children change, replace node (but don't call f() again on the replaced node)
  //
  // assumptions: nodes don't change too much in structure ast-wise
  def topDownTransformationWithParent(n: Node)(f: (Option[Node], Node) => (Option[Node], Boolean)): Node =
    topDownTransformation0(None, n)(f)

  def topDownTransformation0(p: Option[Node], n: Node)(f: (Option[Node], Node) => (Option[Node], Boolean)): Node = {
    val (nCopy, keepGoing) = f(p, n)
    val newNode = nCopy getOrElse n
    if (!keepGoing) return newNode
    def recur[N0 <: Node](n0: N0) =
      topDownTransformation0(Some(newNode), n0)(f).asInstanceOf[N0]
    newNode match {
      case node @ SelectStmt(p, r, f, g, o, _, _) =>
        node.copy(projections = p.map(recur),
                  relations = r.map(_.map(recur)),
                  filter = f.map(recur),
                  groupBy = g.map(recur),
                  orderBy = o.map(recur))
      case node @ ExprProj(e, _, _) => node.copy(expr = recur(e))
      case node @ Or(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ And(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Eq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Neq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Ge(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Gt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Le(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Lt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ In(e, s, _, _) => node.copy(elem = recur(e), set = s.map(recur))
      case node @ Like(l, r, _, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Plus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Minus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Mult(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Div(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Not(e, _) => node.copy(expr = recur(e))
      case node @ Exists(s, _) => node.copy(select = recur(s))
      case node @ Subselect(s, _) => node.copy(subquery = recur(s))
      case node @ CountExpr(e, _, _) => node.copy(expr = recur(e))
      case node @ Sum(e, _, _) => node.copy(expr = recur(e))
      case node @ Avg(e, _, _) => node.copy(expr = recur(e))
      case node @ Min(e, _) => node.copy(expr = recur(e))
      case node @ Max(e, _) => node.copy(expr = recur(e))
      case node @ GroupConcat(e, _, _) => node.copy(expr = recur(e))
      case node @ FunctionCall(_, a, _) => node.copy(args = a.map(recur))
      case node @ Extract(e, _, _) => node.copy(expr = recur(e))
      case node @ Substring(e, _, _, _) => node.copy(expr = recur(e))
      case node @ CaseExprCase(c, e, _) => node.copy(cond = recur(c), expr = recur(e))
      case node @ CaseExpr(e, c, d, _) =>
        node.copy(expr = recur(e), cases = c.map(recur), default = d.map(recur))
      case node @ CaseWhenExpr(c, d, _) =>
        node.copy(cases = c.map(recur), default = d.map(recur))
      case node @ UnaryPlus(e, _) => node.copy(expr = recur(e))
      case node @ UnaryMinus(e, _) => node.copy(expr = recur(e))
      case node @ SubqueryRelationAST(s, _, _) => node.copy(subquery = recur(s))
      case node @ JoinRelation(l, r, _, c, _) =>
        node.copy(left = recur(l), right = recur(r), clause = recur(c))
      case node @ SqlGroupBy(k, h, _) => node.copy(keys = k.map(recur), having = h.map(recur))
      case node @ SqlOrderBy(k, _) => node.copy(keys = k.map(x => (recur(x._1), x._2)))
      case e => e
    }
  }
}
