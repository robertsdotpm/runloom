/*
 * selfcheck.als -- Alloy model of runloom's runtime structural invariant.
 *
 * runloom_self_check (src/runloom_c/runloom_diag.c) walks the netpoll parker graph
 * at runtime and flags four structural violations:
 *   1. a cycle in the global parker list,
 *   2. a per-fd bucket that self-loops,
 *   3. parked_total (atomic) != number of parkers on the global list,
 *   4. a bucket entry that is NOT on the global list (a leaked/dangling link).
 *
 * Those are exactly the relational "shape" properties Alloy checks
 * exhaustively over a bounded universe.  This model formalizes the invariant
 * once, and the two checks below mirror the verify/ negative-control style:
 *   - WellFormedImpliesOK : the well-formedness the runtime maintains (every
 *     parker on the global list, acyclic) IMPLIES the self_check invariant.
 *     Expected: VALID (no counterexample) within the scope.
 *   - BucketsAlwaysOnGlobal: the claim "any acyclic bucket layout already has
 *     every bucket entry on the global list" -- which is FALSE: a buggy unlink
 *     that drops a parker from the global list but leaves it in its bucket
 *     produces a dangling bucket entry.  Expected: COUNTEREXAMPLE found, the
 *     exact shape self_check's "bucket entries not in global list" catches.
 */
module selfcheck

sig Parker {
  gnext: lone Parker,   -- successor in the global parker list
  bnext: lone Parker    -- successor within this parker's per-fd bucket chain
}

one sig GlobalHead { ghead: lone Parker }   -- head of the global list
sig Bucket { bhead: lone Parker }           -- one per fd; head of its chain

-- parkers reachable along the global list / along any bucket chain
fun onGlobal : set Parker { GlobalHead.ghead.*gnext }
fun inBuckets : set Parker { Bucket.bhead.*bnext }

pred NoGlobalCycle      { no p: Parker | p in p.^gnext }   -- invariant 1
pred NoBucketSelfLoop   { no p: Parker | p.bnext = p }     -- invariant 2
pred BucketsSubsetGlobal{ inBuckets in onGlobal }          -- invariant 4

-- the full self_check invariant (count-consistency #3 is implied here by
-- modelling the global list as the single source of truth for membership)
pred SelfCheckOK { NoGlobalCycle and NoBucketSelfLoop and BucketsSubsetGlobal }

-- what the runtime actually maintains: every parker is linked on the global
-- list, the list is acyclic, and no bucket self-loops.
pred WellFormed {
  Parker = onGlobal
  NoGlobalCycle
  NoBucketSelfLoop
}

-- (A) the maintained well-formedness is sufficient for the invariant.
assert WellFormedImpliesOK { WellFormed => SelfCheckOK }
check WellFormedImpliesOK for 7

-- (B) NEGATIVE CONTROL: an acyclic, self-loop-free layout does NOT by itself
-- put every bucket entry on the global list -- a buggy unlink leaves a
-- dangling bucket entry.  Alloy must find this counterexample.
assert BucketsAlwaysOnGlobal { (NoGlobalCycle and NoBucketSelfLoop) => BucketsSubsetGlobal }
check BucketsAlwaysOnGlobal for 7
