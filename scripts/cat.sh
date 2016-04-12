#!/bin/bash
# Copyright © 2016 Alain Kägi

# Exit on errors.
set -e

function usage
{
    echo "usage: cat.sh source target"
    echo ""
    echo "Concatenate files produced by the LDBC SNB data generator.  This"
    echo "command expects the source directory to contain the generated files"
    echo "with names of pattern ENTITY[_0-9]*.csv where ENTITY might contain"
    echo "underscores.  When the command completes, the initially non-existing"
    echo "target directory will contain the concatenated content with"
    echo "deduplicated headers in filenames with pattern ENTITY.csv."
}

##
# Merge files of a certain pattern found in $src into one in $dst
# @param $1  Entity to be merged
# The input files in $src are assumed to follow the naming pattern
# ${entity}[_0-9]*.csv.  The function also assumes each file in $src
# has an identical header and that this header is included only once
# in the output file.
function merge
{
    local entity=$1

    local all_files=`ls $src/*.csv | grep "^$src/$entity[_0-9]*\.csv"`

    if [ -z "$all_files" ]; then
        echo "$entity: No such entity" >&2
        exit 1
    fi

    local first_file=`echo $all_files | sed 's, .*,,'`

    local header=`sed 1q $first_file`

    echo $header > $dst/$entity.csv

    for file in $all_files; do
        tail -n +2 $file >> $dst/$entity.csv
    done
}

# Check usage.
if [ $# -lt 2 -o -e "$2" ]; then
    usage >&2
    exit 1
fi

src=$1
dst=$2

mkdir -p $dst

merge comment
merge forum
merge organisation
merge person
merge place
merge post
merge tag
merge tagclass
merge comment_hasCreator_person
merge comment_hasTag_tag
merge comment_isLocatedIn_place
merge comment_replyOf_comment
merge comment_replyOf_post
merge forum_containerOf_post
merge forum_hasMember_person
merge forum_hasModerator_person
merge forum_hasTag_tag
merge organisation_isLocatedIn_place
merge person_email_emailaddress
merge person_hasInterest_tag
merge person_isLocatedIn_place
merge person_knows_person
merge person_likes_comment
merge person_likes_post
merge person_speaks_language
merge person_studyAt_organisation
merge person_workAt_organisation
merge place_isPartOf_place
merge post_hasCreator_person
merge post_hasTag_tag
merge post_isLocatedIn_place
merge tag_hasType_tagclass
merge tagclass_isSubclassOf_tagclass
