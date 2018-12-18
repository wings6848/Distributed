#!/bin/bash

echo ""
echo "==> Part I"
go test -run Sequential ../mapreduce/...

echo ""
echo "==> Part II"
(./test-wc.sh > /dev/null)

echo ""
echo "==> Part III"
go test -run TestBasic ../mapreduce/...

echo ""
echo "==> Part IV"
go test -run Failure ../mapreduce/...

echo ""
echo "==> Part V (challenge)"
(./test-ii.sh > /dev/null)

rm ./mrtmp.* ./diff.out