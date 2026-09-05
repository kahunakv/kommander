# Promoted failures

A plan in this folder is a failure somebody kept. `TestPlanRegressions` replays every one of them.

## How to promote a failure

1. Find the artifact. A failing generated run writes `dst-artifacts/random-seed-N.plan.txt` beside
   the test binary. When the shrinker ran, a shorter file sits next to it.
2. Copy the file into this folder. Prefer the shrunk one: it says the same thing in fewer actions,
   and it carries the same header.
3. Rename it so the name says what it is, for example `finding-3-compaction-wedge.plan.txt`. The
   file name is what a failure message quotes.

Nothing else is needed. The project file copies `*.plan.txt` from this folder to the output
directory, and the corpus is read from there at run time.

## Why the plan and not the seed

A seed fixes the draws, and a draw depends on what the generator observed. These runs drive real
clusters whose nodes own their own threads, so two runs of one seed can observe different leaders and
diverge from there. A seed promoted as a regression tests a different run every night. The plan
removes the generator from the loop: the same actions in the same order, whatever the cluster does
between them.

## What the header is for

The lines above the actions record the seed and the bounds the run was drawn under. A replay uses
them. A plan replayed at three steps per action is not the plan that failed at six, so a file that
lost its header would be a test of something nobody recorded.

A header line the current build does not know about is ignored, and a bound the file does not carry
falls back to its default. That is deliberate: a plan promoted before a knob existed must stay
loadable the day somebody adds one.

## One replay is weak evidence

These plans are not deterministic, and a plan is promoted here precisely because it reproduced
*intermittently*. Set `KOMMANDER_DST_REPLAY_REPEATS` to replay each plan several times.

Size that number from the plan's measured rate, not from a round number. A plan that reproduces
nine times in a hundred is caught by three replays only one run in four, and by twenty-four replays
about nine runs in ten. The nightly job uses twenty-four.

## An empty corpus is a failure, not a pass

`TestPlanRegressions` fails when it loads no plan at all. It did not always: it returned early on an
empty corpus, and that hid a broken copy rule for as long as the rule stayed wrong — the test read
nothing and passed in twenty-one milliseconds.

So if the test reports that nothing loaded, check three things in this order.

1. The copy rule in `Kommander.Tests.csproj`. The output folder beside the test binary must hold a
   `regressions/` directory with the plans in it.
2. `KOMMANDER_DST_REPLAY_DIR`, which overrides the folder when it is set.
3. Whether somebody removed every plan on purpose. If so, remove the assertion in the same change.
