# Memgraph Workflow

This chapter describes the usual workflow for working on Memgraph.

## Git

Memgraph uses [git](https://git-scm.com/) for source version control. If you
obtained the source, you probably already have it installed. Before you can
track new changes, you need to setup some basic information.

First, tell git your name:

    git config --global user.name "FirstName LastName"

Then, set your Memgraph email:

    git config --global user.email "my.email@memgraph.com"

Finally, make git aware of your favourite editor:

    git config --global core.editor "vim"

## Github

All of the code in Memgraph needs to go through code review before it can be
accepted in the codebase. This is done through [Github](https://github.com/).
You should already have it installed if you followed the steps in [Quick
Start](quick-start.md).

## Working on Your Feature Branch

Git has a concept of source code **branches**. The `master` branch contains all
of the changes which were reviewed and accepted in Memgraph's code base. The
`master` branch is selected by default.

### Creating a Branch

When working on a new feature or fixing a bug, you should create a new branch
out of the `master` branch. There are two branch types, **epic** and **task**
branches. The epic branch is created when introducing a new feature or any work
unit requiring more than one commit. More commits are required to split the
work into chunks to be able to easier review code or find a bug (in each
commit, there could be various problems, e.g., related to performance or
concurrency issues, which are the hardest to track down). Each commit on the
master or epic branch should be a compilable and well-documented set of
changes. Task branches should be created when a smaller work unit has to be
integrated into the codebase. The task branch could be branched out of the
master or an epic branch. We manage epics and tasks on the project management
tool called [Airtable](https://airtable.com/tblTUqycq8sHTTkBF). Each epic is
prefixed by `Exyz-MG`, on the other hand, each task has `Tabcd-MG` prefix.
Examples on how to create branches follow:

```
git checkout master
git checkout -b T0025-MG-fix-a-problem
...
git checkout master
git checkout -b E025-MG-huge-feature
...
git checkout E025-MG-huge-feature
git checkout -b T0123-MG-add-feature-part
```

Note that a branch is created from the currently selected branch. So, if you
wish to create another branch from `master` you need to switch to `master`
first.

### Making and Committing Changes

When you have a branch for your new addition, you can now actually start
implementing it. After some amount of time, you may have created new files,
modified others and maybe even deleted unused files. You need to tell git to
track those changes. This is accomplished with `git add` and `git rm`
commands.

    git add path-to-new-file path-to-modified-file
    git rm path-to-deleted-file

To check that everything is correctly tracked, you may use the `git status`
command. It will also print the name of the currently selected branch.

If everything seems OK, you should commit these changes to git.

    git commit

You will be presented with an editor where you need to type the commit
message. Writing a good commit message is an art in itself. You should take a
look at the links below. We try to follow these conventions as much as
possible.

  * [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/)
  * [A Note About Git Commit Messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)
  * [stopwritingramblingcommitmessages](http://stopwritingramblingcommitmessages.com/)

### Sending Changes on a Review

After finishing your work on your feature branch, you will want to send it on
code review. This is done by pushing the branch to Github and creating a pull
request. You can find all PRs
[here](https://github.com/memgraph/memgraph/pulls).

### Code Integration

When working, you have to integrate some changes to your work or push your work
to be available for others. To pull changes into a local `branch`, usually run
the following:

    git checkout {{branch}}
    git pull origin {{branch}}

To push your changes, usually run the following:

    git checkout {{branch}}
    git push origin {{branch}}

Sometimes, things could get a little bit more complicated. Diagram below shows
which git operation should be performed if a piece of code has to be integrated
from one branch to another. Note, `main_branch` is the **master** branch in our
case.

```
      |<---------------------------|
      |       squash merge         |
      |--------------------------->|
      |          merge             |
      |                            |
      |<-----------|<--------------|
      |   merge    | squash merge  |
      |            |               |
      |----------->|-------------->|
      |   rebase   |     merge     |
      |            | rebase --onto |
      |            |               |
 main_branch  epic_branch     task_branch
```

There are a couple of cases:

* If a code has to be integrated from a task branch to the main branch, use
  **squash merge**. While you were working on a task, you probably committed a
couple of cleanup commits that are not relevant to the main branch. In the
other direction, while integrating the main branch to a task branch, the
**regular merge** is ok because changes from the task branch will later be
squash merged.

* You should use **squash merge** when integrating changes from task to epic
  branch (task might have irrelevant commits). On the other hand, you should
use a **regular merge** when an epic is completed and has to be integrated into
the main branch. Epic is a more significant piece of work, decoupled in
compilable and testable commits. All these commits should be preserved to be
able to find potential issues later on.

* You should use **rebase** when integrating changes from main to an epic
  branch. The epic branch has to be as clean as possible, avoid pure merge
commits. Once you rebase epic on main, all commits on the epic branch will
change the hashes. The implications are: 1) you have to force push your local
branch to the origin, 2) if you made a task branch out of the epic branch, you
would have to use **rebase --onto** (please refer to `git help rebase` for
details). In simple cases, **regular merge** should be sufficient to integrate
changes from epic to a task branch (that can even be done via GitHub web
interface).

During any code integration, you may get reports that some files have
conflicting changes. If you need help resolving them, don't be afraid to ask
around! After you've resolved them, mark them as done with `git add` command.
You may then continue with `git {{action}} --continue`.
