---
title: 'Detecting event patterns <span class="label label-danger" style="font-size:50%">Experimental</span>'
nav-parent_id: streaming_tableapi
nav-title: 'Detecting event patterns'
nav-pos: 5
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

It is a common use-case to search for set event patterns, especially in case of data streams. Apache Flink
comes with [CEP library]({{ site.baseurl }}/dev/libs/cep.html) which allows for pattern detection in event streams. On the other hand Flink's 
Table API & SQL provides a relational way to express queries that comes with multiple functions and 
optimizations that can be used out of the box. In December 2016, ISO released a new version of the 
international SQL standard (ISO/IEC 9075:2016) including the Row Pattern Recognition for complex event processing,
which allowed to consolidate those two APIs using MATCH_RECOGNIZE clause.

* This will be replaced by the TOC
{:toc}

Example query
-------------

Row Pattern Recognition in SQL is performed using the MATCH_RECOGNIZE clause. MATCH_RECOGNIZE enables you to do the following tasks:
* Logically partition and order the data that is used in the MATCH_RECOGNIZE clause with its PARTITION BY and ORDER BY clauses.
* Define patterns of rows to seek using the PATTERN clause of the MATCH_RECOGNIZE clause. 
  These patterns use regular expression syntax, a powerful and expressive feature, applied to the pattern variables you define.
* Specify the logical conditions required to map a row to a row pattern variable in the DEFINE clause.
* Define measures, which are expressions usable in other parts of the SQL query, in the MEASURES clause.

For example to find periods of constantly decreasing price of a Ticker one could write a query like this:

{% highlight sql %}
SELECT *
FROM Ticker 
MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY rowtime
    MEASURES  
       STRT_ROW.rowtime AS start_tstamp,
       LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,
       LAST(PRICE_UP.rowtime) AS end_tstamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST UP
    PATTERN (STRT_ROW PRICE_DOWN+ PRICE_UP+)
    DEFINE
       PRICE_DOWN AS PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1) OR
               (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < STRT_ROW.price))
       PRICE_UP AS PRICE_UP.price > LAST(PRICE_UP.price, 1) OR LAST(PRICE_UP.price, 1) IS NULL
    ) MR;
{% endhighlight %}

This query given following input data:
 
{% highlight text %}
SYMBOL         ROWTIME         PRICE
======  ====================  =======
'ACME'  '01-Apr-11 10:00:00'   12
'ACME'  '01-Apr-11 10:00:01'   17
'ACME'  '01-Apr-11 10:00:02'   19
'ACME'  '01-Apr-11 10:00:03'   21
'ACME'  '01-Apr-11 10:00:04'   25
'ACME'  '01-Apr-11 10:00:05'   12
'ACME'  '01-Apr-11 10:00:06'   15
'ACME'  '01-Apr-11 10:00:07'   20
'ACME'  '01-Apr-11 10:00:08'   24
'ACME'  '01-Apr-11 10:00:09'   25
'ACME'  '01-Apr-11 10:00:10'   19
{% endhighlight %}

will produce a summary row for each found period in which the price was constantly decreasing.

{% highlight text %}
SYMBOL          START_TST          BOTTOM_TS         END_TSTAM
=========  ==================  ==================  ==================
ACME       01-APR-11 10:00:04  01-APR-11 10:00:05  01-APR-11 10:00:09
{% endhighlight %}

The aforementioned query consists of following clauses:

* [PARTITION BY](#partitioning) - defines logical partitioning of the stream, similar to `GROUP BY` operations.
* [ORDER BY](#order-of-events) - specifies how should the incoming events be order, this is essential as patterns define order.
* [MEASURES](#define--measures) - defines output of the clause, similar to `SELECT` clause
* [ONE ROW PER MATCH](#output-mode) - output mode which defines how many rows per match will be produced
* [AFTER MATCH SKIP](#after-match-skip) - allows to specify where next match should start, this is also a way to control to how many distinct matches a single event can belong
* [PATTERN](#defining-pattern) - clause that allows constructing patterns that will be searched for, pro 
* [DEFINE](#define--measures) - this section defines conditions on events that should be met in order to be qualified to corresponding pattern variable


Installation guide
------------------

Match recognize uses Apache Flink's CEP library internally. In order to be able to use this clause one has to add
this library as dependency. Either by adding it to your uber-jar by adding dependency on:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

or by adding it to the cluster classpath (see [here]({{ site.baseurl}}/dev/linking.html)). If you want to use
MATCH_RECOGNIZE from [sql-client]({{ site.baseurl}}/dev/table/sqlClient.html) you don't have to do anything as all the dependencies are included by default.

Partitioning
------------
It is possible to look for patterns in a partitioned data, e.g. trends for a single ticker. This can be expressed using `PARTITION BY` clause. It is equivalent to applying
[`keyBy`]({{ site.baseurl }}/dev/stream/operators/index.html#datastream-transformations) transformation to a `DataStream`, or similar to using `GROUP BY` for aggregations.

<span class="label label-danger" style="font-size:75%">Attention:</span> It is highly advised to apply partitioning because otherwise `MATCH_RECOGNIZE` will be translated
into a non-parallel operator to ensure global ordering.

Order of events
---------------

Apache Flink allows searching for patterns based on time, either [processing-time or event-time](time_attributes.html). This assumption
is very important, because it allows sorting events, before fed into pattern state machine. Because of that one may be true
that the produced output will be correct in regards to order in which those events happened.

As a consequence one has to provide time indicator as the first argument to `ORDER BY` clause.

That means for a table:

{% highlight text %}
Ticker
     |-- symbol: Long
     |-- price: Long
     |-- tax: Long
     |-- rowTime: TimeIndicatorTypeInfo(rowtime)
{% endhighlight %}

Definition like:
{% highlight sql %}
ORDER BY rowtime, price
{% endhighlight %}
Would be a valid one, but
{% highlight sql %}
ORDER BY price
{% endhighlight %}
would throw exception. An exception will be thrown as well if the rowtime order is not the primary one:
{% highlight sql %}
ORDER BY price, rowtime
{% endhighlight %}
It is also not possible to define descending order for time indicator, but it is allowed for any subsequent secondary sorting, therefore this is a valid expression:
{% highlight sql %}
ORDER BY rowtime ASC, price DESC
{% endhighlight %}
but this isn’t:
{% highlight sql %}
ORDER BY rowtime DESC, price DESC
{% endhighlight %}

Define & Measures
-----------------

`DEFINE` and `MEASURES` keywords have similar functions as `WHERE` and `SELECT` clauses in a simple SQL query.

Using `MEASURES` clause you can define what will be included in the output of the clause. What exactly will be produced depends also
on the [output mode](#output-mode) setting.

On the other hand `DEFINE` allows to specify conditions that rows have to fulfill in order to be classified to according [pattern variable](#defining-pattern).

For more thorough explanation on expressions that you can use in those clauses please have a look at [event stream navigation](#event-stream-navigation).

Defining pattern
----------------

MATCH_RECOGNIZE clause allows user to search for patterns in event streams using a powerful and expressive language
that is somewhat similar to widespread regular expression syntax. Every pattern is constructed from building blocks called
pattern variables, to whom operators (quantifiers and other modifiers) can be applied. The whole pattern must be enclosed in
brackets. Example pattern:

{% highlight sql %}
PATTERN (A B+ C*? D)
{% endhighlight %}

One may use the following operators:

* Concatenation - a pattern like (A B) means that between the A B the contiguity is strict. This means there can be no rows that were not mapped to A or B in between
* Quantifiers - modifies the number of rows that can be mapped to pattern variable
  * `*` — 0 or more rows
  * `+` — 1 or more rows
  * `?` — 0 or 1 rows
  * `{ n }` — exactly n rows (n > 0)
  * `{ n, }` — n or more rows (n ≥ 0)
  * `{ n, m }` — between n and m (inclusive) rows (0 ≤ n ≤ m, 0 < m)
  * `{ , m }` — between 0 and m (inclusive) rows (m > 0)
  
<span class="label label-danger" style="font-size:75%">Note:</span> Patterns that can potentially produce empty match are not supported.
Examples of such patterns are: `(A*)`, `(A? B*)`, `(A{0,} B{0,} C*)` etc.

### Greedy & reluctant quantifiers

Each quantifier can be either greedy (true by default) or reluctant. The difference is that greedy quantifiers try to match
as many rows as possible, while reluctant as few as possible. To better illustrate the difference one can analyze following example:

Query with greedy quantifier applied to `B` variable:
{% highlight sql %}
SELECT * 
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            C.price as lastPrice
        PATTERN (A B* C)
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        DEFINE
            A as A.price > 10
            B as B.price < 15
            C as B.price > 12
    )
{% endhighlight %}

For input:

{% highlight text %}
 symbol  tax   price          rowtime
======= ===== ======== =====================
 XYZ     1     10       2018-09-17 10:00:02
 XYZ     2     11       2018-09-17 10:00:03
 XYZ     1     12       2018-09-17 10:00:04
 XYZ     2     13       2018-09-17 10:00:05
 XYZ     1     14       2018-09-17 10:00:06
 XYZ     2     16       2018-09-17 10:00:07
{% endhighlight %}

Will produce output:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      16
{% endhighlight %}

but the same query with just the `B*` modified to `B*?`, which means it should be reluctant quantifier, will produce:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      13
{% endhighlight %}

<span class="label label-danger" style="font-size:75%">Note:</span> It is not possible to use greedy quantifier for the last
variable for a pattern, thus pattern like `(A B*)` is not allowed. This can be easily worked around by introducing artificial state
e.g. `C` that will have a negated condition of `B`. So you could use a query like:

{% highlight sql %}
PATTERN (A B* C)
DEFINE
    A as condA()
    B as condB()
    C as NOT condB()
{% endhighlight %}

<span class="label label-danger" style="font-size:75%">Note:</span> Right now optional reluctant quantifier (`A??` or `A{0,1}?`) is not supported.

Output mode
-----------

Currently supported output mode is `ONE ROW PER MATCH` that will always produce on output summary row per each found match.
The schema of the output row will be following union of `{partitioning columns} + {measures columns}` in that particular order.

Example:

Query:
{% highlight sql %}
SELECT * 
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            FIRST(A.price) as startPrice
            LAST(A.price) as topPrice
            B.price as lastPrice
        PATTERN (A+ B)
        ONE ROW PER MATCH
        DEFINE
            A as A.price > LAST(A.price, 1) OR LAST(A.price, 1) IS NULL,
            B as B.price < LAST(A.price)
    )
{% endhighlight %}

for input:

{% highlight text %}
 symbol   tax   price.         rowtime
======== ===== ======== =====================
 XYZ      1     10       2018-09-17 10:00:02
 XYZ      2     12       2018-09-17 10:00:03
 XYZ      1     13       2018-09-17 10:00:04
 XYZ      2     11       2018-09-17 10:00:05
{% endhighlight %}

will produce:

{% highlight text %}
 symbol   startPrice   topPrice   lastPrice
======== ============ ========== ===========
 XYZ      10           13         11
{% endhighlight %}

Event stream navigation
------------------

### Pattern variable reference
Pattern variable reference e.g. A.price describes a set of rows mapped so far to A plus current row, if we try to match current row to A. If the expression requires a single row e.g. A.price > 10, A.price selects the last value of the row.

If no pattern variable is specified e.g. SUM(price) it references the default pattern variable which references all variables in the pattern, so it constitutes row set of all rows mapped so far plus current row.

Example:

{% highlight sql %}
...
PATTERN (A B+)
DEFINE
  A as A.price > 10,
  B as B.price > A.price AND SUM(price) < 100 AND SUM(B.price) < 80
{% endhighlight %}
  
By `{A.price}` we describe set of rows that the values in DEFINE clause are evaluated on. The rows are referenced by its number

<table class="table table-bordered">
  <thead>
    <tr>
      <th>no</th>
      <th>price</th>
      <th>CLAS</th>
      <th>{A.price}</th>
      <th>{B.price}</th>
      <th>{price}</th>
      <th>A.price</th>
      <th>B.price</th>
      <th>SUM(price)</th>
      <th>SUM(B.price)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>10</td>
      <td>-&gt; A</td>
      <td>1</td>
      <td>-</td>
      <td>-</td>
      <td>10</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr>
      <td>2</td>
      <td>15</td>
      <td>-&gt; B</td>
      <td>1</td>
      <td>2</td>
      <td>1,2</td>
      <td>10</td>
      <td>15</td>
      <td>25</td>
      <td>15</td>
    </tr>
    <tr>
      <td>3</td>
      <td>20</td>
      <td>-&gt; B</td>
      <td>1</td>
      <td>2,3</td>
      <td>1,2,3</td>
      <td>10</td>
      <td>20</td>
      <td>45</td>
      <td>35</td>
    </tr>
    <tr>
      <td>4</td>
      <td>31</td>
      <td>-&gt; B</td>
      <td>1</td>
      <td>2,3,4</td>
      <td>1,2,3,4</td>
      <td>10</td>
      <td>31</td>
      <td>76</td>
      <td>66</td>
    </tr>
    <tr>
      <td>5</td>
      <td>35</td>
      <td></td>
      <td>1</td>
      <td>2,3,4,5</td>
      <td>1,2,3,4,5</td>
      <td>10</td>
      <td>35</td>
      <td>111</td>
      <td>101</td>
    </tr>
  </tbody>
</table>

### Pattern variable indexing

Apache Flink allows navigating within events that were mapped to a particular pattern variable with so called logical offsets. This can be expressed
with two corresponding functions:

<div data-lang="SQL" markdown="1">
  <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 40%">Comparison functions</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        {% highlight text %}
LAST(variable.field, n)
{% endhighlight %}
      </td>
      <td>
        <p>Returns value of the field from the event that was mapped to n-th element of variable, counting from the last element mapped.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
FIRST(variable.field, n)
{% endhighlight %}
      </td>
      <td>
        <p>Returns value of the field from the event that was mapped to n-th element of variable, counting from the first element mapped.</p>
      </td>
    </tr>
    </tbody>
  </table>

Example:
{% highlight sql %}
...
PATTERN (A B+)
DEFINE
  A as A.price > 10,
  B as (B.price > LAST(B.price, 1) OR LAST(B.price, 1) IS NULL) AND
       (B.price > 2 * LAST(B.price, 2) OR LAST(B.price, 2) IS NULL)
{% endhighlight %}

Will evaluate this expression for each incoming row as follows:

<table class="table table-bordered">
  <thead>
    <tr>  
        <th style="white-space:nowrap">no</th>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">CLAS</th>
        <th style="white-space:nowrap">LAST(B.price, 1)</th>
        <th style="white-space:nowrap">LAST(B.price, 2)</th>
        <th>Comment</th>
    </tr>  
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>10</td>
      <td>-&gt; A</td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>2</td>
      <td>15</td>
      <td>-&gt; B</td>
      <td>null</td>
      <td>null</td>
      <td>Notice that LAST(A.price, 1) is null, because there is still nothing mapped to B</td>
    </tr>
    <tr>
      <td>3</td>
      <td>20</td>
      <td>-&gt; B</td>
      <td>15</td>
      <td>null</td>
      <td></td>
    </tr>
    <tr>
      <td>4</td>
      <td>31</td>
      <td>-&gt; B</td>
      <td>20</td>
      <td>15</td>
      <td></td>
    </tr>
    <tr>
      <td>5</td>
      <td>35</td>
      <td></td>
      <td>31</td>
      <td>20</td>
      <td>Not mapped because 35 &lt; 2 * 20</td>
    </tr>
  </tbody>
</table>

It is also possible to use multiple pattern column reference but all of them must reference the same variable. In other words the value of `LAST`/`FIRST` must be computed in a single row.

Thus it is possible to use:
{% highlight sql %}
LAST(A.price * A.tax)
{% endhighlight %}
But this will throw exception:
{% highlight sql %}
LAST(A.price * B.tax)
{% endhighlight %}

After match skip
----------------

This clause specifies where to start a new matching procedure after a complete match was found.

There are five different strategies:
* `SKIP TO NEXT ROW` - continues searching for new match starting at the next element to the starting element of a match
* `SKIP PAST LAST ROW` - resumes pattern matching at the next row after the last row of the current match.
* `SKIP TO FIRST variable` - resume pattern matching at the first row that is mapped to the pattern variable
* `SKIP TO LAST variable` - resume pattern matching at the last row that is mapped to the pattern variable

This is also a way to specify to how many matches a single event can belong, e.g. with the `SKIP PAST LAST ROW` each event can belong to at most one match.

To better understand differences between those strategies one can analyze following example:

Query:

{% highlight sql %}
SELECT * 
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            SUM(A.price) as sumPrice,
            FIRST(A.rowtime) as startTime,
            LAST(A.rowtime) as endTime
        PATTERN (A+ C)
        ONE ROW PER MATCH
        [AFTER MATCH STRATEGY]
        DEFINE
            A as SUM(A.price) < 30
    )
{% endhighlight %}

For input:

{% highlight text %}
 symbol   tax   price         rowtime
======== ===== ======= =====================
 XYZ      1     7       2018-09-17 10:00:01
 XYZ      2     9       2018-09-17 10:00:02
 XYZ      1     10      2018-09-17 10:00:03
 XYZ      2     17      2018-09-17 10:00:04
 XYZ      2     14      2018-09-17 10:00:05
{% endhighlight %}

Will produce results based on used `AFTER MATCH STRATEGY`:

* AFTER MATCH SKIP PAST LAST ROW

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO LAST A

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      27         2018-09-17 10:00:03   2018-09-17 10:00:04
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO NEXT ROW

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      19         2018-09-17 10:00:02   2018-09-17 10:00:03
 XYZ      27         2018-09-17 10:00:03   2018-09-17 10:00:04
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO FIRST A

This combination will produce runtime exception, because one would try to always start new match where the
last one started. This would produce an ifinite loop, thus it is prohibited.

One has to have in mind that in case of `SKIP TO FIRST/LAST variable` it might be possible that there are no rows mapped to that
variable.(e.g. For pattern A*). In such cases a RuntimeException will be thrown as the standard requires a valid row to continue
matching.

Known limitations
-----------------
 
`MATCH_RECOGNIZE` clause is still an ongoing effort and therefore some SQL Standard features are not supported yet.
List of such features includes:
* pattern expressions
  * pattern groups - this means that e.g. quantifiers can not be applied to subsequence of the pattern, thus `(A (B C)+)` is not a valid pattern
  * alterations - patterns like `PATTERN((A B | C D) E)`, which means that either subsequence `A B` or `C D` has to found before looking for `E` row
  * `PERMUTE` operator - which is equivalent to all permutations of variables that it was applied e.g. `PATTERN (PERMUTE (A, B, C))` = `PATTERN (A B C | A C B | B A C | B C A | C A B | C B A)`
  * anchors - `^, $`, which denote beginning/end of partition, those do not make sense in streaming context and will not be supported
  * exclusion - `PATTERN ({- A -} B)` meaning that `A` will be looked for, but will not participate in the output, this works only for `ALL ROWS PER MATCH` mode
  * reluctant optional quantifier - `PATTERN A??` only greedy optional quantifier is supported
* `ALL ROWS PER MATCH` output mode, which produces output row for every row that participated in creating a found match. This also means:
  * that the only supported semantic for `MEASURES` clause is `FINAL`
  * `CLASSIFIER` function, which returns the pattern variable that row was mapped to, is not yet supported
* `SUBSET` - which allows creating logical groups of pattern variables and using those groups in `DEFINE` and `MEASURES` clauses
* physical offsets - `PREV/NEXT`, which index all events seen rather than only those that were mapped to a pattern variable(as in [logical offsets](#pattern-variable-indexing) case)
* there is no support of aggregates yet, one cannot use aggregates in `MEASURES` nor `DEFINE` clauses
* user defined functions cannot be used within `MATCH_RECOGNIZE`
* `MATCH_RECOGNIZE` is supported only for SQL, there is no equivalent in the table API


### Controlling memory consumption

Memory consumption is an important aspect when writing `MATCH_RECOGNIZE` queries, as the space of potential of potential matches in built in a breadth first like manner.
Having that in mind one must make sure that the pattern can finish(preferably with reasonable number of rows mapped to the match, as they have to fit into memory) e.g. it does not have a quantifier without
upper limit that accepts every single row.

One has to be aware that `MATCH_RECOGNIZE` clause does not use [state retention time](query_configuration.html#idle-state-retention-time). There is also no possibility to
define time restriction on the pattern to finish as of now, as there is no such possibility in SQL standard. Community is in the process of designing a proper syntax for that
feature right now.
