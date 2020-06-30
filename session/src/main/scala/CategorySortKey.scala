
case class CategorySortKey(clickCount:Long, orderCount:Long, payCount:Long) extends Ordered[CategorySortKey]{

  override def compare(that: CategorySortKey): Int = {
    if(this.clickCount - that.clickCount != 0)
      (this.clickCount - that.clickCount).toInt
    else if(this.orderCount - that.orderCount != 0)
      (this.orderCount - that.orderCount).toInt
    else
      (this.payCount -that.payCount).toInt
  }
}
