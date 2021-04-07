import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit, when, round, col}

object Main extends App{

  // Building Spark Session
  val Spark = SparkSession.builder()
                          .master("local")
                          .appName("etl_orders")
                          .getOrCreate()

  // Import implicits conversions for spark
  import Spark.implicits._

  // Path of source data
  val DataPath = "/home/flaviodiasc/Workspaces/Estudo/spark/data/"

  // Defining classes to use as schema in read (Spark Dataset uses it)
  case class OrdersSchema(OrderID: Integer, CustomerID: String, EmployeeID: Integer, OrderDate: String,
                          RequiredDate: String, ShippedDate: String, ShipVia: Integer, Freight: Double,
                          ShipName: String, ShipAddress: String, ShipCity: String, ShipRegion: String,
                          ShipPostalCode: String, ShipCountry: String)
  case class OrderDetailsSchema(OrderID: Integer, ProductID: Integer, UnitPrice: Double,  Quantity:Integer,
                                Discount: Double)
  case class ProductsSchema(ProductID: Integer, ProductName: String, SupplierID: Integer, CategoryID: Integer,
                            QuantityPerUnit: String, UnitPrice: Double, UnitsInStock: Integer, UnitsOnOrder: Integer,
                            ReorderLevel: Integer, Discontinued: Integer)
  case class CategoriesSchema(CategoryID: Integer, CategoryName: String, Description: String, Picture: String)
  case class CustomersSchema(CustomerID: String, CompanyName: String, ContactName: String, ContactTitle: String,
                             Address: String, City: String, Region: String, PostalCode: String, Country: String,
                             Phone: String, Fax: String)
  case class EmployeesSchema(EmployeeID: Integer, LastName: String, FirstName: String, Title: String,
                             TitleOfCourtesy: String, BirthDate: String, HireDate: String, Address: String,
                             City: String, Region: String, PostalCode: String, Country: String, HomePhone: String,
                             Extension: Integer, Photo: String, Notes: String, ReportsTo: String, PhotoPath: String)

  // Reading orders.csv
  val Orders = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "orders.csv")
    .as[OrdersSchema]

  // Reading order-details.csv
  val OrderDetails = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "order-details.csv")
    .as[OrderDetailsSchema]

  // Reading products.csv
  val Products = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "products.csv")
    .as[ProductsSchema]

  // Reading categories.csv
  val Categories = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "categories.csv")
    .as[CategoriesSchema]

  // Reading customers.csv
  val Customers = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "customers.csv")
    .as[CustomersSchema]

  // Reading employees.csv
  val Employees = Spark.read
    .option("inferschema", "true")
    .option("header", "true")
    .option("delimiter", "|")
    .csv(DataPath + "employees.csv")
    .as[EmployeesSchema]

  // Defining keys of relationships to join all sources
  val OrdersRelOrdersDetails = Orders("OrderID") === OrderDetails("OrderID")
  val OrdersRelCustomers = Orders("CustomerID") === Customers("CustomerID")
  val OrdersRelEmployees = Orders("EmployeeID") === Employees("EmployeeID")
  val OrdersDetailsRelProducts = OrderDetails("ProductID") === Products("ProductID")
  val ProductsRelCategories = Products("CategoryID") === Categories("CategoryID")

  // Dataframes Joins, Columns transformations e select.
  val OrdersAllDetails = Orders.join(OrderDetails, OrdersRelOrdersDetails, "inner")
    .join(Customers, OrdersRelCustomers, "inner")
    .join(Employees, OrdersRelEmployees, "inner")
    .join(Products, OrdersDetailsRelProducts, "inner")
    .join(Categories, ProductsRelCategories, "inner")
    .filter(
      (Customers("Country") =!= "Brazil")
      && (Customers("Country") =!= "Venezuela")
    )
    .withColumn("FullName", concat(Employees("FirstName"), lit(" "), Employees("LastName")))
    .withColumn("TotalValue", round((OrderDetails("UnitPrice") * OrderDetails("Quantity")) *
        when(OrderDetails("Discount") === 0, 1).otherwise(OrderDetails("Discount")), 2))
    .withColumn("CustomerCountry", when(Customers("Country") === "UK", "England")
        .otherwise(Customers("Country")))
    .withColumn("EmployeeCountry", when(Employees("Country") === "UK", "England")
        .otherwise(Employees("Country")))
    .select(
      Orders("OrderID"),
      Customers("CustomerID"),
      Customers("CompanyName"),
      col("CustomerCountry"),
      Employees("EmployeeID"),
      col("FullName"),
      Employees("Title"),
      col("EmployeeCountry"),
      Products("ProductID"),
      Products("ProductName"),
      Categories("CategoryID"),
      Categories("CategoryName"),
      OrderDetails("UnitPrice"),
      OrderDetails("Quantity"),
      OrderDetails("Discount"),
      col("TotalValue")
    )

  // Show result set
  OrdersAllDetails.show()
}
