import org.specs2.mutable._

class ResolverSpec extends Specification {

  object resolver extends Resolver

  private def doTest(q: String) = {
    val parser = new SQLParser
    val r = parser.parse(q)
    resolver.resolve(r.get, TestSchema.definition)
  }

  "Resolver" should {
    "resolve query1" in {
      val s0 = doTest(Queries.q1)
      s0.ctx.projections.size must_== 10
    }

    "resolve query2" in {
      val s0 = doTest(Queries.q2)
      s0.ctx.projections.size must_== 8
    }

    "resolve query3" in {
      val s0 = doTest(Queries.q3)
      s0.ctx.projections.size must_== 4
    }

    "resolve query4" in {
      val s0 = doTest(Queries.q4)
      s0.ctx.projections.size must_== 2
    }

    "resolve query5" in {
      val s0 = doTest(Queries.q5)
      s0.ctx.projections.size must_== 2
    }

    "resolve query6" in {
      val s0 = doTest(Queries.q6)
      s0.ctx.projections.size must_== 1
    }

    "resolve query7" in {
      val s0 = doTest(Queries.q7)
      s0.ctx.projections.size must_== 4
    }

    "resolve query8" in {
      val s0 = doTest(Queries.q8)
      s0.ctx.projections.size must_== 2
    }

    "resolve query9" in {
      val s0 = doTest(Queries.q9)
      s0.ctx.projections.size must_== 3
    }

    "resolve query10" in {
      val s0 = doTest(Queries.q10)
      s0.ctx.projections.size must_== 8
    }

    "resolve query11" in {
      val s0 = doTest(Queries.q11)
      s0.ctx.projections.size must_== 2
    }

    "resolve query12" in {
      val s0 = doTest(Queries.q12)
      s0.ctx.projections.size must_== 3
    }

    "resolve query13" in {
      val s0 = doTest(Queries.q13)
      s0.ctx.projections.size must_== 2
    }

    "resolve query14" in {
      val s0 = doTest(Queries.q14)
      s0.ctx.projections.size must_== 1
    }

    "resolve query16" in {
      val s0 = doTest(Queries.q16)
      s0.ctx.projections.size must_== 4
    }

    "resolve query17" in {
      val s0 = doTest(Queries.q17)
      s0.ctx.projections.size must_== 1
    }

    "resolve query18" in {
      val s0 = doTest(Queries.q18)
      s0.ctx.projections.size must_== 6
    }

    "resolve query19" in {
      val s0 = doTest(Queries.q19)
      s0.ctx.projections.size must_== 1
    }

    "resolve query20" in {
      val s0 = doTest(Queries.q20)
      s0.ctx.projections.size must_== 2
    }

    "resolve query21" in {
      val s0 = doTest(Queries.q21)
      s0.ctx.projections.size must_== 2
    }

    "resolve query22" in {
      val s0 = doTest(Queries.q22)
      s0.ctx.projections.size must_== 3
    }
  }
}
