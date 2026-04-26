$creds = @(
    @{ username = "admin@yanbao.local"; password = "Password123" },
    @{ username = "admin@yanbao.local"; password = "password123" },
    @{ username = "admin@yanbao.local"; password = "Admin123" },
    @{ username = "admin@yanbao.local"; password = "Admin123!" }
)

foreach ($c in $creds) {
    Write-Host "Testing $($c.username) / $($c.password)..."
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:8010/auth/login" -Method Post -Body ($c | ConvertTo-Json) -ContentType "application/json" -ErrorAction Stop
        Write-Host "Status: $($resp.StatusCode), Access Token: $(($resp.Content | ConvertFrom-Json).access_token)"
    } catch {
        $status = if ($_.Exception.Response) { $_.Exception.Response.StatusCode } else { "No Response" }
        Write-Host "JSON Method - Status: $status ($($_.Exception.Message))"
        
        # Try again with form data if JSON fails with 422, 415 or 400
        Write-Host "Trying with form data..."
        try {
            $form = @{ username = $c.username; password = $c.password }
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:8010/auth/login" -Method Post -Body $form -ErrorAction Stop
            Write-Host "Status: $($resp.StatusCode), Access Token: $(($resp.Content | ConvertFrom-Json).access_token)"
        } catch {
            $statusForm = if ($_.Exception.Response) { $_.Exception.Response.StatusCode } else { "No Response" }
            Write-Host "Form Method - Status: $statusForm ($($_.Exception.Message))"
        }
    }
    Write-Host "---"
}
