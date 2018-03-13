package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

func (a *Agent) Install() {
	if os.Getuid() != 0 {
		log.Fatal("Please run '", os.Args[0], " install' as root")
	}

	if _, err := os.Stat("pgbackup.conf"); os.IsNotExist(err) {
		log.Fatal("pgbackup.conf does not exist, run '", os.Args[0], " setup' first.")
	}

	os.MkdirAll("/usr/local/bin", 0755)
	err := copyFile(os.Args[0], "/usr/local/bin/pgbackup")
	if err != nil {
		log.Fatal(err)
	}
	os.Chown("/usr/local/bin/pgbackup", 0, 0)
	os.Chmod("/usr/local/bin/pgbackup", 0755)

	log.Print("Installed ", os.Args[0], " to /usr/local/bin/pgbackup")

	if _, err := os.Stat("/etc/pgbackup.conf"); !os.IsNotExist(err) {
		log.Print("Not overwriting existing /etc/pgbackup.conf")
	} else {
		err := copyFile("pgbackup.conf", "/etc/pgbackup.conf")
		if err != nil {
			log.Fatal(err)
		}
		os.Chown("/etc/pgbackup.conf", 0, 0)
		os.Chmod("/etc/pgbackup.conf", 0644)
		log.Print("Copied pgbackup.conf to /etc/pgbackup.conf")
	}

	err = ioutil.WriteFile("/etc/systemd/system/pgbackup.service", []byte(`[Unit]
Description=pgbackup

[Service]
User=postgres
Group=postgres
Type=simple
Restart=always
WorkingDirectory=/
ExecStart=/usr/local/bin/pgbackup agent

[Install]
WantedBy=multi-user.target`), 0644)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Wrote /etc/systemd/system/pgbackup.service")
	cmd := exec.Command("/bin/systemctl", "daemon-reload")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()

	log.Print("Starting pgbackup service")
	cmd = exec.Command("/bin/systemctl", "start", "pgbackup")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Done")
}

func copyFile(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
