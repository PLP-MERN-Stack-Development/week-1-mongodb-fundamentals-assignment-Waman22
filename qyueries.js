// queries.js - MongoDB queries for PLP Bookstore assignment
// Run this in mongosh or MongoDB Compass

// Switch to plp_bookstore database
use('plp_bookstore');

// Task 3: Advanced Queries
// Note: No books in the dataset are published after 2010. Using >1950 for demonstration.
print('Books in stock and published after 1950:');
db.books.find({
  $and: [
    { in_stock: true },
    { published_year: { $gt: 1950 } }
  ]
}).pretty();

// Use projection to return only title, author, and price fields for in-stock books
print('In-stock books with title, author, and price:');
db.books.find(
  { in_stock: true },
  { title: 1, author: 1, price: 1, _id: 0 }
).pretty();

// Sort books by price (ascending)
print('Books sorted by price (ascending):');
db.books.find().sort({ price: 1 }).pretty();

// Sort books by price (descending)
print('Books sorted by price (descending):');
db.books.find().sort({ price: -1 }).pretty();

// Pagination: 5 books per page (first page)
print('First page (5 books):');
db.books.find().skip(0).limit(5).pretty();

// Pagination: 5 books per page (second page)
print('Second page (5 books):');
db.books.find().skip(5).limit(5).pretty();

// Task 4: Aggregation Pipeline
// Calculate the average price of books by genre
print('Average price by genre:');
db.books.aggregate([
  {
    $group: {
      _id: '$genre',
      averagePrice: { $avg: '$price' }
    }
  },
  {
    $sort: { averagePrice: -1 }
  }
]).pretty();

// Find the author with the most books in the collection
print('Author with the most books:');
db.books.aggregate([
  {
    $group: {
      _id: '$author',
      bookCount: { $sum: 1 }
    }
  },
  {
    $sort: { bookCount: -1 }
  },
  {
    $limit: 1
  }
]).pretty();

// Group books by publication decade and count them
print('Books by publication decade:');
db.books.aggregate([
  {
    $bucket: {
      groupBy: '$published_year',
      boundaries: [1800, 1810, 1820, 1830, 1840, 1850, 1860, 1870, 1880, 1890, 1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010],
      default: 'Other',
      output: {
        count: { $sum: 1 }
      }
    }
  },
  {
    $sort: { _id: 1 }
  }
]).pretty();

// Task 5: Indexing
// Create an index on the title field
print('Creating index on title:');
db.books.createIndex({ title: 1 });

// Create a compound index on author and published_year
print('Creating compound index on author and published_year:');
db.books.createIndex({ author: 1, published_year: 1 });

// Verify indexes
print('Indexes on books collection:');
db.books.getIndexes();

// Use explain() to demonstrate performance improvement
print('Explain: Query using title index:');
db.books.find({ title: 'The Hobbit' }).explain('executionStats');

print('Explain: Query using compound index:');
db.books.find({ author: 'J.R.R. Tolkien', published_year: { $gt: 1930 } }).explain('executionStats');
