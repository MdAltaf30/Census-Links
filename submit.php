<?php
// Check if the form is submitted
if ($_SERVER["REQUEST_METHOD"] == "POST") {
    // Retrieve form data
    $name = $_POST['name'];
    $email = $_POST['email'];
    $password = $_POST['password'];
    $gender = $_POST['gender'];
    $subscribe = isset($_POST['subscribe']) ? 'Yes' : 'No'; // Checkbox
    $message = $_POST['message'];
    $country = $_POST['country'];
    $age = $_POST['age'];
    $birthdate = $_POST['birthdate'];
    $color = $_POST['color'];
    $volume = $_POST['volume'];
    // File upload handling
    $file_uploaded = false;
    if(isset($_FILES['file'])) {
        $file_name = $_FILES['file']['name'];
        $file_tmp = $_FILES['file']['tmp_name'];
        move_uploaded_file($file_tmp, "uploads/" . $file_name);
        $file_uploaded = true;
    }
    // Hidden field
    $hiddenField = $_POST['hiddenField'];

    // Now you can do whatever you want with this data
    // For example, you can save it to a database, send it via email, etc.

    // Dummy example: print the data
    echo "Name: $name <br>";
    echo "Email: $email <br>";
    echo "Gender: $gender <br>";
    echo "Subscribe to Notifications: $subscribe <br>";
    echo "Comments: $message <br>";
    echo "Country: $country <br>";
    echo "Age: $age <br>";
    echo "Birthdate: $birthdate <br>";
    echo "Favorite Color: <div style='width:20px;height:20px;background-color:$color'></div> <br>";
    echo "Overall Percentage: $volume% <br>";
    echo "File Uploaded: " . ($file_uploaded ? "Yes" : "No") . "<br>";
    echo "Hidden Field: $hiddenField <br>";
}
?>
