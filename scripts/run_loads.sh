#!/bin/bash

bptree_basic="bptree_basic_analysis"
bptree_mixed="bptree_mixed_analysis"
dual_basic="dual_basic_analysis"
dual_mixed="dual_mixed_analysis"

show_usage() {
  echo "Usage: ./run_loads <tree_type> <test_type> <input_dir> <output_dir> [config_file] [num_queries] [prec_loads] [num_total_ops]"
  echo "<tree_type> := \"bplus\" | \"dual\""
  echo "<test_type> := \"basic\" | \"mixed\". Basic test only runs insertions while mixed runs queries either."
  echo "<input_dir> := Path to the directory that only stores test_file create by bods generator."
  echo "<output_dir> := Path to the directory that stores the analysis results"
  echo "[config_file] := Path to the configuration file of dual tree. Only used when the tree_type is \"dual\"."
  echo "[num_queries] := Number of queires needed to run. Only used when the test_type is \"mixed\"."
  echo "[prec_loads] := Percentage of the data that will be preloaded before timing the test. Only used when the test_type is \"mixed\"."
  echo "[num_total_ops] := Number of operations(queries and insertions) that will be performed, and usually it equals to the size of the test set. Only used when the test_type is \"mixed\"."
}

# $3 is tree_type, $4 is test_type, $1 is input_dir, $2 is output_dir. After $5 is an array, it put all arguments that
#needed by specified tree_type and test_type combination
analysis_folder() {
  for f in "$1"/*; do
    echo "Analyzing $f"
    if [ -d $f ]; then
      case $# in
      4)
        analysis_folder $f $2 $3 $4
        ;;
      5)
        analysis_folder $f $2 $3 $4 $5
        ;;
      7)
        analysis_folder $f $2 $3 $4 $5 $6 $7
        ;;
      8)
        analysis_folder $f $2 $3 $4 $5 $6 $7 $8
        ;;
      *)
        echo "Error match analysis_folder parameters"
        exit 1
        ;;
      esac
    else
      output_file_name="$2/$3_$4_${1////-}-res"
      if [ "$3" = "dual" ]; then
        if [ "$4" = "basic" ]; then
          echo "dual_basic $f $5 >> $output_file_name"
          ./$dual_basic $f $5 >>$output_file_name
        else
          echo "dual_mixed $f $5 $6 $7 $8 >> $output_file_name"
          ./$dual_mixed $f $5 $6 $7 $8 >>$output_file_name
        fi
      else
        if [ "$4" = "basic" ]; then
          echo "bptree_basic $f >> $output_file_name"
          ./$bptree_basic $f >>$output_file_name
        else
          echo "bptree_mixed $f $5 $6 $7 >> $output_file_name"
          ./$bptree_mixed $f $5 $6 $7 >>$output_file_name
        fi
      fi
    fi
  done
}

if [ $# -lt 4 ]; then
  echo "At least 4 arguments are needed"
  show_usage
  exit 1
fi

if [ "$1" != "bplus" -a "$1" != "dual" ]; then
  echo "Invalid tree type \"$1\"."
  show_usage
  exit 1
fi

if [ "$2" != "basic" -a "$2" != "mixed" ]; then
  echo "Invalid test type \"$2\"."
  show_usage
  exit 1
fi

if [ ! -d "$3" ]; then
  echo "Cannot find directory \"$3\"."
  show_usage
  exit 1
fi

if [ ! -d "$4" ]; then
  echo "Create output directory \"$4\"."
  mkdir $4
fi

if [ "$1" = "dual" ]; then
  if [ $# -lt 4 ]; then
    echo "Dual tree must use a configuration."
    show_usage
    exit 1
  fi
  config_file="$5"
else
  config_file=""
fi

if [ "$2" = "basic" ]; then
  if [ "$1" = "dual" ]; then
    echo "Prepare to execute dual tree basic analysis"
    analysis_folder $3 $4 $1 $2 $config_file
  else
    echo "Prepare to execute bptree basic analysis"
    analysis_folder $3 $4 $1 $2
  fi
else
  if [ "$1" = "dual" ]; then
    echo "Prepare to execute dual tree mixed analysis"
    if [ $# -lt 8 ]; then
      echo "Missing arguments."
      show_usage
      exit 1
    fi
    analysis_folder $3 $4 $1 $2 $config_file $6 $7 $8
  else
    echo "Prepare to execute bptree mixed analysis"
    if [ $# -lt 7 ]; then
      echo "Missing arguments."
      show_usage
      exit 1
    fi
    analysis_folder $3 $4 $1 $2 $5 $6 $7
  fi
fi
