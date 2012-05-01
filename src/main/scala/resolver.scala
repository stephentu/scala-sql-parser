trait Resolver extends Transformers with Traversals {
  case class ResolutionException(msg: String) extends RuntimeException(msg)

  def resolve(stmt: SelectStmt, schema: Definitions): SelectStmt = {
    // init contexts
    val n1 = topDownTransformationWithParent(stmt)((parent: Option[Node], child: Node) => child match {
      case s: SelectStmt =>
        parent match {
          case _: SubqueryRelationAST =>
            (Some(s.copyWithContext(new Context(Left(schema)))), true)
          case _ =>
            (Some(s.copyWithContext(new Context(parent.map(c => Right(c.ctx)).getOrElse(Left(schema))))), true)
        }
      case e =>
        (Some(e.copyWithContext(parent.map(_.ctx).getOrElse(throw new RuntimeException("should have ctx")))), true)
    }).asInstanceOf[SelectStmt]

    // build contexts up
    topDownTraversal(n1)(wrapReturnTrue {
      case s @ SelectStmt(projections, relations, _, _, _, _, ctx) =>

        def checkName(name: String, alias: Option[String], ctx: Context): String = {
          val name0 = alias.getOrElse(name)
          if (ctx.relations.contains(name0)) {
            throw ResolutionException("relation " + name0 + " is referenced twice w/ same alias")
          }
          name0
        }

        def processRelation(r: SqlRelation): Unit = r match {
          case TableRelationAST(name, alias, _) =>

            //println("processing: " + name)
            //println(ctx.relations)

            // check name
            val name0 = checkName(name, alias, ctx)

            // check valid table
            val r = schema.defns.get(name).getOrElse(
              throw ResolutionException("no such table: " + name))

            // add relation to ctx
            ctx.relations += (name0 -> TableRelation(name))

          case SubqueryRelationAST(subquery, alias, ctxInner) =>
            // check name
            val name0 = checkName(alias, None, ctx)

            // add relation to ctx
            ctx.relations += (name0 -> SubqueryRelation(subquery))

          case JoinRelation(left, right, _, _, _) =>
            processRelation(left)
            processRelation(right)

        }

        relations.map(_.foreach(processRelation))

        var seenWildcard = false
        projections.zipWithIndex.foreach {
          case (ExprProj(f @ FieldIdent(qual, name, _, _), alias, _), idx) =>
            ctx.projections += NamedProjection(alias.getOrElse(name), f, idx)
          case (ExprProj(expr, alias, _), idx) =>
            ctx.projections += NamedProjection(alias.getOrElse("$unknown$"), expr, idx)
          case (StarProj(_), _) if !seenWildcard =>
            ctx.projections += WildcardProjection
            seenWildcard = true
          case _ =>
        }

      case _ => (None, true)
    })

    // resolve field idents
    def resolveFIs(ss: SelectStmt): SelectStmt = {
      def visit[N <: Node](n: N, allowProjs: Boolean): N = {
        topDownTransformation(n) {
          case f @ FieldIdent(qual, name, _, ctx) =>
            val cols = ctx.lookupColumn(qual, name, allowProjs)
            if (cols.isEmpty) throw new ResolutionException("no such column: " + f.sql)
            if (cols.size > 1) throw new ResolutionException("ambiguous reference: " + f.sql)
            (Some(FieldIdent(qual, name, cols.head, ctx)), false)
          case ss: SelectStmt =>
            (Some(resolveFIs(ss)), false)
          case _ => (None, true)
        }.asInstanceOf[N]
      }
      val SelectStmt(p, r, f, g, o, _, _) = ss
      ss.copy(
        projections = p.map(p0 => visit(p0, false)),
        relations = r.map(_.map(r0 => visit(r0, false))),
        filter = f.map(f0 => visit(f0, false)),
        groupBy = g.map(g0 => g0.copy(keys = g0.keys.map(k => visit(k, true)), having = g0.having.map(h => visit(h, false)))),
        orderBy = o.map(o0 => visit(o0, true)))
    }

    val res = resolveFIs(n1)

    def fixContextProjections(ss: SelectStmt): Unit = {
      val x = ss.ctx.projections.map {
        case n @ NamedProjection(_, _, pos) =>
          n.copy(expr = ss.projections(pos).asInstanceOf[ExprProj].expr)
        case e => e
      }
      ss.ctx.projections.clear
      ss.ctx.projections ++= x
    }

    topDownTraversal(res)(wrapReturnTrue {
      case ss: SelectStmt => fixContextProjections(ss)
      case _ =>
    })
    res
  }
}
