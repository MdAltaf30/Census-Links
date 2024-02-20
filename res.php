<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Form Details</title>
</head>
<body>
    <h2>Form Details</h2>
    <?php
    if ($_SERVER["REQUEST_METHOD"] == "POST") {
        echo "Name: " . $_POST["name"] . "<br>";
        echo "Email: " . $_POST["email"] . "<br>";
        echo "Age: " . $_POST["age"] . "<br>";
        echo "Gender: " . $_POST["gender"] . "<br>";
        echo "Country: " . $_POST["country"] . "<br>";
        echo "City: " . $_POST["city"] . "<br>";
        echo "Address: " . $_POST["address"] . "<br>";
        echo "Phone: " . $_POST["phone"] . "<br>";
        echo "Hobby 1: " . $_POST["hobby1"] . "<br>";
        echo "Hobby 2: " . $_POST["hobby2"] . "<br>";
        echo "Hobby 3: " . $_POST["hobby3"] . "<br>";
        echo "Hobby 4: " . $_POST["hobby4"] . "<br>";
        echo "Hobby 5: " . $_POST["hobby5"] . "<br>";
    }
    ?>
</body>
</html>
