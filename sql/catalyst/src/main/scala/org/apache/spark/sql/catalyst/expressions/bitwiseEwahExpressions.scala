/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import com.googlecode.javaewah.EWAHCompressedBitmap

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/*
  * A function that return the bit wise AND operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise AND",
  extended = "> SELECT _FUNC_(array(13,2,4,8589934592,4096,0), array(13,2,4,8589934592,4096,0);")
case class EwahBitmapAnd(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback{

   override def dataType: DataType = StringType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    var a = 0;
    for ( a <- 0 until  bitmap_1.asInstanceOf[GenericArrayData].numElements()) {
      b1.addWord(bitmap_1.asInstanceOf[GenericArrayData].getLong(a))
    }

    var b = 0;
    for ( b <- 0 until  bitmap_2.asInstanceOf[GenericArrayData].numElements()) {
      b2.addWord(bitmap_2.asInstanceOf[GenericArrayData].getLong(b))
    }

    val result: EWAHCompressedBitmap = b1.and(b2)
    return result.toString
  }
}


/*
  * A function that return the bit wise OR operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise OR",
  extended = "> SELECT _FUNC_(array(13,2,4,8589934592,4096,0), array(13,2,4,8589934592,4096,0);")
case class EwahBitmapOr(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback{

  override def dataType: DataType = StringType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    var a = 0;
    for ( a <- 0 until  bitmap_1.asInstanceOf[GenericArrayData].numElements()) {
      b1.addWord(bitmap_1.asInstanceOf[GenericArrayData].getLong(a))
    }

    var b = 0;
    for ( b <- 0 until  bitmap_2.asInstanceOf[GenericArrayData].numElements()) {
      b2.addWord(bitmap_2.asInstanceOf[GenericArrayData].getLong(b))
    }

    val result: EWAHCompressedBitmap = b1.or(b2)
    return result.toString
  }
}

/*
  * A function that return the count of bits set to true in the given Ewah Bitmap
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1) - Returns the count of bits set to true",
  extended = "> SELECT _FUNC_(array(13,2,4,8589934592,4096,0));")
case class EwahBitmapCount(child: Expression)
  extends UnaryExpression with CodegenFallback{

  override def dataType: DataType = IntegerType

  override def nullSafeEval(bitmap_1: Any): Any = {

    val bitmap: EWAHCompressedBitmap = new EWAHCompressedBitmap

    var a = 0;
    for ( a <- 0 until  bitmap_1.asInstanceOf[GenericArrayData].numElements()) {
      bitmap.addWord(bitmap_1.asInstanceOf[GenericArrayData].getLong(a))
    }

    return bitmap.cardinality()
  }
}