/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GroupingOperation
        extends Expression
{
    private final List<Expression> groupingColumns;

    public GroupingOperation(Optional<NodeLocation> location, List<Expression> groupingColumns)
    {
        super(location);
        requireNonNull(groupingColumns);
        checkArgument(!groupingColumns.isEmpty(), "grouping operation columns cannot be empty");
        if (groupingColumns.size() > 63) {
            throw new IllegalArgumentException("More than 63 grouping columns are not allowed as the resulting bit set won't fit in a Java long data type.");
        }
        this.groupingColumns = ImmutableList.copyOf(groupingColumns);
    }

    public List<Expression> getGroupingColumns()
    {
        return groupingColumns;
    }

    public String getName()
    {
        return "grouping_operation";
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingOperation(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupingOperation other = (GroupingOperation) o;
        return Objects.equals(groupingColumns, other.groupingColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupingColumns);
    }
}
