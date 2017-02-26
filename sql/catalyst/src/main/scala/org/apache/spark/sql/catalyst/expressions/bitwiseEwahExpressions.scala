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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/*
  * A function that return the bit wise AND operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise AND",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0');")
case class EwahBitmapAnd(left: Expression, right: Expression)
  extends BinaryExpression {

  override def dataType: DataType = left.dataType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }
    val result: EWAHCompressedBitmap = b1.and(b2)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (left, right) => {
      val bitWords1 = ctx.freshName("bitWords")
      val bitWords2 = ctx.freshName("bitWords")
      val bitmap1 = ctx.freshName("bitmap")
      val bitmap2 = ctx.freshName("bitmap")
      val i = ctx.freshName("i")
      val EWAHCompressedBitmap = classOf[EWAHCompressedBitmap].getName

      s"""
         |
         | int $i = 0;
         | String[] $bitWords1 = $left.toString().split(",");
         | String[] $bitWords2 = $right.toString().split(",");
         | $EWAHCompressedBitmap $bitmap1 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap2 = new $EWAHCompressedBitmap();
         |
         | for ($i = 0; $i < $bitWords1.length; $i++) {
         |   if($i== $bitWords1.length-1) {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), true);
         |   }
         |   else {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords2.length; $i++) {
         |   if($i== $bitWords2.length-1) {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), true);
         |   }
         |   else {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), false);
         |   }
         | }
         |
         |  ${ev.value} = UTF8String.fromString($bitmap1.and($bitmap2).toRLWString());
       """.stripMargin
    })
  }
}

/*
  * A function that return the bit wise AND operation between three Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2, bitmap3) - Returns the bitwise AND",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0', " +
    "'13,2,4,8589934592,4096,0');")
case class EwahBitmapAndTer(first: Expression, second: Expression, third: Expression)
  extends TernaryExpression {

  override def dataType: DataType = first.dataType
  override def children: Seq[Expression] = Seq(first, second, third)

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any, bitmap_3: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b3: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")
    val bitmap3_String = bitmap_3.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }

    var c = 0;
    for (c <- 0 until bitmap3_String.length) {
      if (c == bitmap3_String.length - 1) {
        b3.setBufferWord(c, java.lang.Long.parseLong(bitmap3_String(c)), true)
      }
      else {
        b3.setBufferWord(c, java.lang.Long.parseLong(bitmap3_String(c)), false)
      }
    }

    val result: EWAHCompressedBitmap = b1.and(b2).and(b3)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (first, second, third) => {
      val bitWords1 = ctx.freshName("bitWords1")
      val bitWords2 = ctx.freshName("bitWords2")
      val bitWords3 = ctx.freshName("bitWords3")
      val bitmap1 = ctx.freshName("bitmap1")
      val bitmap2 = ctx.freshName("bitmap2")
      val bitmap3 = ctx.freshName("bitmap3")
      val i = ctx.freshName("i")
      val EWAHCompressedBitmap = classOf[EWAHCompressedBitmap].getName

      s"""
         |
         | int $i = 0;
         | String[] $bitWords1 = $first.toString().split(",");
         | String[] $bitWords2 = $second.toString().split(",");
         | String[] $bitWords3 = $third.toString().split(",");
         | $EWAHCompressedBitmap $bitmap1 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap2 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap3 = new $EWAHCompressedBitmap();
         |
         | for ($i = 0; $i < $bitWords1.length; $i++) {
         |   if($i== $bitWords1.length-1) {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), true);
         |   }
         |   else {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords2.length; $i++) {
         |   if($i== $bitWords2.length-1) {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), true);
         |   }
         |   else {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords3.length; $i++) {
         |   if($i== $bitWords3.length-1) {
         |      $bitmap3.setBufferWord($i, java.lang.Long.parseLong($bitWords3[$i]), true);
         |   }
         |   else {
         |      $bitmap3.setBufferWord($i, java.lang.Long.parseLong($bitWords3[$i]), false);
         |   }
         | }
         |
         |  ${ev.value} = UTF8String.fromString($bitmap1.and($bitmap2).and($bitmap3).toRLWString());
       """.stripMargin
    })
  }

}


/*
  * A function that return the bit wise AND operation between three Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2, bitmap3) - Returns the bitwise AND",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0', " +
    "'13,2,4,8589934592,4096,0');")
case class EwahBitmapOrTer(first: Expression, second: Expression, third: Expression)
  extends TernaryExpression {

  override def dataType: DataType = first.dataType
  override def children: Seq[Expression] = Seq(first, second, third)

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any, bitmap_3: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b3: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")
    val bitmap3_String = bitmap_3.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }

    var c = 0;
    for (c <- 0 until bitmap3_String.length) {
      if (c == bitmap3_String.length - 1) {
        b3.setBufferWord(c, java.lang.Long.parseLong(bitmap3_String(c)), true)
      }
      else {
        b3.setBufferWord(c, java.lang.Long.parseLong(bitmap3_String(c)), false)
      }
    }

    val result: EWAHCompressedBitmap = b1.or(b2).or(b3)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (first, second, third) => {
      val bitWords1 = ctx.freshName("bitWords1")
      val bitWords2 = ctx.freshName("bitWords2")
      val bitWords3 = ctx.freshName("bitWords3")
      val bitmap1 = ctx.freshName("bitmap1")
      val bitmap2 = ctx.freshName("bitmap2")
      val bitmap3 = ctx.freshName("bitmap3")
      val i = ctx.freshName("i")
      val EWAHCompressedBitmap = classOf[EWAHCompressedBitmap].getName

      s"""
         |
         | int $i = 0;
         | String[] $bitWords1 = $first.toString().split(",");
         | String[] $bitWords2 = $second.toString().split(",");
         | String[] $bitWords3 = $third.toString().split(",");
         | $EWAHCompressedBitmap $bitmap1 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap2 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap3 = new $EWAHCompressedBitmap();
         |
         | for ($i = 0; $i < $bitWords1.length; $i++) {
         |   if($i== $bitWords1.length-1) {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), true);
         |   }
         |   else {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords2.length; $i++) {
         |   if($i== $bitWords2.length-1) {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), true);
         |   }
         |   else {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords3.length; $i++) {
         |   if($i== $bitWords3.length-1) {
         |      $bitmap3.setBufferWord($i, java.lang.Long.parseLong($bitWords3[$i]), true);
         |   }
         |   else {
         |      $bitmap3.setBufferWord($i, java.lang.Long.parseLong($bitWords3[$i]), false);
         |   }
         | }
         |
         |  ${ev.value} = UTF8String.fromString($bitmap1.or($bitmap2).or($bitmap3).toRLWString());
       """.stripMargin
    })
  }

}
/*
  * A function that return the bit wise OR operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise OR",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0');")
case class EwahBitmapOr(left: Expression, right: Expression)
  extends BinaryExpression  {

  override def dataType: DataType = left.dataType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {


    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }
    val result: EWAHCompressedBitmap = b1.or(b2)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }


  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (left, right) => {
      val bitWords1 = ctx.freshName("bitWords")
      val bitWords2 = ctx.freshName("bitWords")
      val bitmap1 = ctx.freshName("bitmap")
      val bitmap2 = ctx.freshName("bitmap")
      val i = ctx.freshName("i")
      val EWAHCompressedBitmap = classOf[EWAHCompressedBitmap].getName

      s"""
         |
         | int $i = 0;
         | String[] $bitWords1 = $left.toString().split(",");
         | String[] $bitWords2 = $right.toString().split(",");
         | $EWAHCompressedBitmap $bitmap1 = new $EWAHCompressedBitmap();
         | $EWAHCompressedBitmap $bitmap2 = new $EWAHCompressedBitmap();
         |
         | for ($i = 0; $i < $bitWords1.length; $i++) {
         |   if($i== $bitWords1.length-1) {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), true);
         |   }
         |   else {
         |      $bitmap1.setBufferWord($i, java.lang.Long.parseLong($bitWords1[$i]), false);
         |   }
         | }
         |
         | for ($i = 0; $i < $bitWords2.length; $i++) {
         |   if($i== $bitWords2.length-1) {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), true);
         |   }
         |   else {
         |      $bitmap2.setBufferWord($i, java.lang.Long.parseLong($bitWords2[$i]), false);
         |   }
         | }
         |
         |  ${ev.value} = UTF8String.fromString($bitmap1.or($bitmap2).toRLWString());
       """.stripMargin
    })
  }
}

/*
  * A function that return the count of bits set to true in the given Ewah Bitmap
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1) - Returns the count of bits set to true",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0');")
case class EwahBitmapCount(child: Expression)
  extends UnaryExpression {

  override def dataType: DataType = IntegerType

  override def nullSafeEval(bitmap_1: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }
    return b1.cardinality()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      val bitWords = ctx.freshName("bitWords")
      val bitmap = ctx.freshName("bitmap")
      val i = ctx.freshName("i")
      val EWAHCompressedBitmap = classOf[EWAHCompressedBitmap].getName

      s"""
         | int $i = 0;
         | String[] $bitWords = $eval.toString().split(",");
         | $EWAHCompressedBitmap $bitmap = new $EWAHCompressedBitmap();
         |
         | for ($i = 0; $i < $bitWords.length; $i++) {
         |   if($i== $bitWords.length-1) {
         |      $bitmap.setBufferWord($i, java.lang.Long.parseLong($bitWords[$i]), true);
         |   }
         |   else {
         |      $bitmap.setBufferWord($i, java.lang.Long.parseLong($bitWords[$i]), false);
         |   }
         | }
         |  ${ev.value} = $bitmap.cardinality();
       """.stripMargin
    })
  }
}