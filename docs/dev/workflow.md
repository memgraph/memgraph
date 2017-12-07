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

## Phabricator

All of the code in Memgraph needs to go through code review before it can be
accepted in the codebase. This is done through
[Phabricator](https://phacility.com/phabricator/). The command line tool for
interfacing with Phabricator is
[arcanist](https://phacility.com/phabricator/arcanist/). You should already
have it installed if you followed the steps in [Quick Start](quick-start.md).

The only required setup is to go in the root of Memgraph's project and run:

    arc install-certificate

## Working on Your Feature Branch

Git has a concept of source code *branches*. The `master` branch contains all
of the changes which were reviewed and accepted in Memgraph's code base. The
`master` branch is selected by default.

### Creating a Branch

When working on a new feature or fixing a bug, you should create a new branch
out of the `master` branch. For example, let's say you are adding static type
checking to the query language compiler. You would create a branch called
`mg_query_static_typing` with the following command:

    git branch mg_query_static_typing

To switch to that branch, type:

    git checkout mg_query_static_typing

Since doing these two steps will happen often, you can use a shortcut command:

    git checkout -b mg_query_static_typing

Note that a branch is created from the currently selected branch. So, if you
wish to create another branch from `master` you need to switch to `master`
first.

The usual convention for naming your branches is `mg_<feature_name>`, you may
switch underscores ('\_') for hyphens ('-').

Do take care not to mix the case of your branch names! Certain operating
systems (like Windows) don't distinguish the casing in git branches. This may
cause hard to track down issues when trying to switch branches. Therefore, you
should always name your branches with lowercase letters.

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
code review. This is done through Arcanist. To do that, run the following
command:

    arc diff

You will, once again, be presented with an editor where you need to describe
your whole work. `arc` will by default fill that description with your commit
messages. The title and summary of your work should also follow the
conventions of git messages as described above. If you followed the
guidelines, the message filled by `arc` should be fine.

In addition to the message, you need to fill the `Reviewers:` line with
usernames of people who should do the code review.

You changes will be visible on Phabricator as a so called "diff". You can find
the default view of active diffs
[here](https://phabricator.memgraph.io/differential/)

### Updating Changes Based on Review

When you get comments in the code review, you will want to make additional
modifications to your work. The same workflow as before applies: [Making and
Committing Changes](#making-and-committing-changes)

After making those changes, send them back on code review:

    arc diff


### Updating From New Master

Let's say that, while you were working, someone else added some new features
to the codebase that you would like to use in your current work. To obtain
those changes you should update your `master` branch:

    git checkout master
    git pull origin master

Now, these changes are on `master`, but you want them in your local branch. To
do that, use `git rebase`:

    git checkout mg_query_static_typing
    git rebase master

During `git rebase`, you may get reports that some files have conflicting
changes. If you need help resolving them, don't be afraid to ask around! After
you've resolved them, mark them as done with `git add` command. You may
then continue with `git rebase --continue`.

After the `git rebase` is done, you will now have new changes from `master` on
your feature branch as if you just created and started working on that branch.
You may continue with the usual workflow of [Making and Committing
Changes](#making-and-committing-changes) and [Sending Changes on a
Review](#sending-changes-on-a-review).

### Sending Your Changes on Master Branch

When your changes pass the code review, you are ready to integrate them in the
`master` branch. To do that, run the following command:

    arc land

Arcanist will take care of obtaining the latest changes from `master` and
merging your changes on top. If the `land` was successful, Arcanist will
delete your local branch and you will be back on `master`. Continuing from the
examples above, the deleted branch would be `mg_query_static_typing`.

This marks the completion of your changes, and you are ready to work on
something else.

### Note For People Familiar With Git

Since Arcanist takes care of merging your git commits and pushing them on
`master`, you should *never* have to call `git merge` and `git push`. If you
find yourself typing those commands, check that you are doing the right thing.
The most common mistake is to use `git merge` instead of `git rebase` for the
case described in [Updating From New Master](#updating-from-new-master).
