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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class HexToDecimal
{
    private HexToDecimal()
    {
    }

    @Description("Convert a hexadecimal number to its decimal equivalent")
    @ScalarFunction("hex_to_decimal")
    @SqlType(StandardTypes.BIGINT)
    public static long hexToDecimal(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            return Long.parseLong(value.toStringUtf8(), 16);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Input argument incorrectly formatted", e);
        }
    }
}
