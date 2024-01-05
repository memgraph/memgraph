# Architecture Decision Records

Also known as ADRs. This practice has become widespread in many
high performing engineering teams. It is a technique for communicating
between software engineers. ADRs provide a clear and documented
history of architectural choices, ensuring that everyone on the
team is on the same page. This improves communication and reduces
misunderstandings. The act of recording decisions encourages
thoughtful consideration before making choices. This can lead to
more robust and better-informed architectural decisions.

Links must be created, pointing both to and from the Github Issues
and/or the Notion Program Management "Initiative" database.

ADRs are complimentary to any tech specs that get written while
designing a solution. ADRs are very short and to the point, while
tech specs will include diagrams and can be quite verbose.

## HOWTO

Each ADR will be assigned a monotonically increasing unique numeric
identifier, which will be zero-padded to 3 digits. Each ADR will
be in a single markdown file containing no more than one page of
text, and the filename will start with that unique identifier,
followed by a camel case phrase summarizing the problem.  For
example: `001_architecture_decision_records.md` or
`002_big_integration_cap_theorem.md`.

We want to use an ADR when:
1. Significant Impact: This includes choices that affect scalability, performance, or fundamental design principles.
1. Long-Term Ramifications: When a decision is expected to have long-term ramifications or is difficult to reverse.
1. Architectural Principles: ADRs are suitable for documenting decisions related to architectural principles, frameworks, or patterns that shape the system's structure.
1. Controversial Choices: When a decision is likely to be controversial or may require justification in the future.

## Do

1. Keep them brief and concise.
1. Explain the trade-offs.
1. Each ADR should be about one AD, not multiple ADs
1. Don't alter existing information in an ADR. Instead, amend the ADR by adding new information, or supersede the ADR by creating a new ADR.
1. Explain your organization's situation and business priorities.
1. Include rationale and considerations based on social and skills makeups of your teams.
1. Include pros and cons that are relevant, and describe them in terms that align with your needs and goals.
1. Explain what follows from making the decision. This can include the effects, outcomes, outputs, follow ups, and more.

## Don't

1. Try to guess what the executive leader wants, and then attempt to please them. Be objective.
1. Try to solve everything all at once. A pretty good solution now is MUCH BETTER than a perfect solution later. Carpe diem!
1. Hide any doubts or unanswered questions.
1. Make it a sales pitch. Everything has upsides and downsides - be authentic and honest about them.
1. Perform merely a superficial investigation. If an ADR doesn't call for some deep thinking, then it probably shouldn't exist.
1. Ignore the long-term costs such as performance, tech debt or hardware and maintenance.
1. Get tunnel vision where creative or surprising approaches are not explored.

# Template - use the format below for each new ADR

1. **Author** - who has written the ADR
1. **Status** - one of: PROPOSED, ACCEPTED, REJECTED, SUPERSEDED-BY or DEPRECATED
1. **Date** - when the status was most recently updated
1. **Problem** - a concise paragraph explaining of the context
1. **Criteria** - a list of the two or three metrics by which the solution was evaluated, and their relative weights (importance)
1. **Decision** - what was chosen as the way forward, and what the consequences are of the decision

