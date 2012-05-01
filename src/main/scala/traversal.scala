trait Traversals {

  def wrapReturnTrue[A](f: Node => A): Node => Boolean = (n: Node) => { f(n); true }

  def topDownTraversal(n: Node)(f: Node => Boolean): Unit =
    topDownTraversalWithParent(n)( (p: Option[Node], c: Node) => f(c) )

  def topDownTraversalWithParent(n: Node)(f: (Option[Node], Node) => Boolean): Unit =
    topDownTraversal0(None, n)((n: Node) => (), f, (n: Node) => ())

  def topDownTraversalPrePost[A0, A1](n: Node)(preVisit: Node => A0, visit: Node => Boolean, postVisit: Node => A1): Unit =
   topDownTraversalPrePostWithParent(n)(preVisit, (p: Option[Node], c: Node) => visit(c), postVisit)

  def topDownTraversalPrePostWithParent[A0, A1](n: Node)(preVisit: Node => A0, visit: (Option[Node], Node) => Boolean, postVisit: Node => A1): Unit =
   topDownTraversal0(None, n)(preVisit, visit, postVisit)

  private def topDownTraversal0[A0, A1](p: Option[Node], n: Node)(preVisit: Node => A0, visit: (Option[Node], Node) => Boolean, postVisit: Node => A1): Unit = {

    preVisit(n)
    if (!visit(p, n)) {
      postVisit(n)
      return
    }

    def recur(n0: Node) = topDownTraversal0(Some(n), n0)(preVisit, visit, postVisit)
    n match {
      case SelectStmt(p, r, f, g, o, _, _) =>
        p.map(recur); r.map(_.map(recur)); f.map(recur); g.map(recur); o.map(recur)
      case ExprProj(e, _, _) => recur(e)
      case Or(l, r, _) => recur(l); recur(r)
      case And(l, r, _) => recur(l); recur(r)
      case Eq(l, r, _) => recur(l); recur(r)
      case Neq(l, r, _) => recur(l); recur(r)
      case Ge(l, r, _) => recur(l); recur(r)
      case Gt(l, r, _) => recur(l); recur(r)
      case Le(l, r, _) => recur(l); recur(r)
      case Lt(l, r, _) => recur(l); recur(r)
      case In(e, s, _, _) => recur(e); s.map(recur)
      case Like(l, r, _, _) => recur(l); recur(r)
      case Plus(l, r, _) => recur(l); recur(r)
      case Minus(l, r, _) => recur(l); recur(r)
      case Mult(l, r, _) => recur(l); recur(r)
      case Div(l, r, _) => recur(l); recur(r)
      case Not(e, _) => recur(e)
      case Exists(s, _) => recur(s)
      case Subselect(s, _) => recur(s)
      case CountExpr(e, _, _) => recur(e)
      case Sum(e, _, _) => recur(e)
      case Avg(e, _, _) => recur(e)
      case Min(e, _) => recur(e)
      case Max(e, _) => recur(e)
      case GroupConcat(e, _, _) => recur(e)
      case FunctionCall(_, a, _) => a.map(recur)
      case Extract(e, _, _) => recur(e)
      case Substring(e, _, _, _) => recur(e)
      case CaseExprCase(c, e, _) => recur(c); recur(e)
      case CaseExpr(e, c, d, _) => recur(e); c.map(recur); d.map(recur)
      case CaseWhenExpr(c, d, _) => c.map(recur); d.map(recur)
      case UnaryPlus(e, _) => recur(e)
      case UnaryMinus(e, _) => recur(e)
      case SubqueryRelationAST(s, _, _) => recur(s)
      case JoinRelation(l, r, _, c, _) => recur(l); recur(r); recur(c)
      case SqlGroupBy(k, h, _) => k.map(recur); h.map(recur)
      case SqlOrderBy(k, _) => k.map(x => recur(x._1))
      case _ =>
    }
    postVisit(n)
  }
}
